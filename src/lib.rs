#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
//! Benzin provides async variants of diesel related query functionality
//!
//! benzin is a rework of [diesel_async](https://docs.rs/diesel-async/latest/diesel_async/)
//! with proper transaction handling (is does not leave dangling transactions in case of a cancelled future)
//!
//! benzin is an extension to diesel itself. It is designed to be used together
//! with the main diesel crate. It only provides async variants of core diesel traits,
//! that perform actual io-work.
//!
//! In addition to these core traits 2 fully async connection implementations are provided
//! by benzin:
//!
//! * [`AsyncMysqlConnection`] (enabled by the `mysql` feature, currently not supported)
//! * [`AsyncPgConnection`] (enabled by the `postgres` feature)
//!
//! Ordinary usage of `benzin` assumes that you just replace the corresponding sync trait
//! method calls and connections with their async counterparts.
//!
//! ```rust
//! # include!("./doctest_setup.rs");
//! #
//! use diesel::prelude::*;
//! use benzin::{RunQueryDsl, AsyncConnection};
//!
//! diesel::table! {
//!    users(id) {
//!        id -> Integer,
//!        name -> Text,
//!    }
//! }
//! #
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! #     run_test().await.unwrap();
//! # }
//! #
//! # async fn run_test() -> QueryResult<()> {
//!
//! use crate::users::dsl::*;
//!
//! # let mut connection = TestConnection::default();
//!
//! # connection.expected_load(vec![vec![String::from("1"), String::from("Sean")],
//! #                                vec![String::from("2"), String::from("Tess")]]);
//! let data = users
//!     // use ordinary diesel query dsl here
//!     .filter(id.gt(0))
//!     // execute the query via the provided
//!     // async variant of `benzin::RunQueryDsl`
//!     .load::<(i32, String)>(&mut connection)
//!     .await?;
//! let expected_data = vec![
//!     (1, String::from("Sean")),
//!     (2, String::from("Tess")),
//! ];
//! assert_eq!(expected_data, data);
//! #     Ok(())
//! # }
//! ```

#![warn(missing_docs)]

use std::pin::Pin;

use diesel::{
    backend::Backend,
    query_builder::{AsQuery, QueryFragment, QueryId},
    row::Row,
    ConnectionResult, QueryResult,
};
use futures_util::Stream;

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod pg;
#[cfg(any(feature = "deadpool", feature = "bb8"))]
pub mod pooled_connection;
mod run_query_dsl;
#[allow(dead_code)]
mod stmt_cache;
mod test_connection;

#[cfg(feature = "mysql")]
#[doc(inline)]
pub use self::mysql::AsyncMysqlConnection;
#[cfg(feature = "postgres")]
#[doc(inline)]
pub use self::pg::AsyncPgConnection;
#[doc(inline)]
pub use self::run_query_dsl::*;

pub use self::test_connection::TestConnection;

/// A basic trait for executing queries
///
/// Perform query and execute operations on a backend.
/// It can be used to query the database through the query dsl
/// provided by diesel, custom extensions or raw sql queries.
pub trait AsyncExecute: Sized + Send {
    /// Execute multiple SQL statements within the same string.
    ///
    /// This function is used to execute migrations,
    /// which may contain more than one SQL statement.
    fn batch_execute(
        &mut self,
        query: &str,
    ) -> impl std::future::Future<Output = QueryResult<()>> + Send;
    /// The inner stream returned by `AsyncConnection::load`
    type Stream<'conn>: Stream<Item = QueryResult<Self::Row<'conn>>> + Send
    where
        Self: 'conn;
    /// The row type used by the stream returned by `AsyncConnection::load`
    type Row<'conn>: Row<'conn, Self::Backend>
    where
        Self: 'conn;
    /// The backend this type connects to
    type Backend: Backend;

    #[doc(hidden)]
    fn load<T>(
        &mut self,
        source: T,
    ) -> impl std::future::Future<Output = QueryResult<Self::Stream<'_>>> + Send
    where
        T: AsQuery + Send,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send;

    #[doc(hidden)]
    fn execute_returning_count<T>(
        &mut self,
        source: T,
    ) -> impl std::future::Future<Output = QueryResult<usize>> + Send
    where
        T: QueryFragment<Self::Backend> + QueryId + Send;
}

/// An async transaction
///
/// This trait represents an async database transaction over an existing connection.
pub trait AsyncTransaction {
    /// Commits the transaction
    fn commit(self) -> impl std::future::Future<Output = QueryResult<()>> + Send;
    /// Rolls back the transaction
    fn rollback(self) -> impl std::future::Future<Output = QueryResult<()>> + Send;
}

/// An async connection to a database
///
/// This trait represents an async database connection.
pub trait AsyncConnection: AsyncExecute {
    /// Establishes a new connection to the database
    ///
    /// The argument to this method and the method's behavior varies by backend.
    /// See the documentation for that backend's connection class
    /// for details about what it accepts and how it behaves.
    fn establish(
        database_url: &str,
    ) -> impl std::future::Future<Output = ConnectionResult<Self>> + Send;
}

/// An async transactional entity
///
/// Allows to execute queries inside a transaction.
/// Can be implemented for specific AsyncConnection (if transactions is supported)
/// as well as for specific AsyncTransaction (if nesting of transactions is supported).
pub trait AsyncTransactional {
    /// The associated transaction type for this connection
    type Transaction<'a>: AsyncTransaction
    where
        Self: 'a;

    /// Begins a new transaction
    ///
    /// Return value should be commited manually otherwise it will be rolled back automatically when dropped
    /// Also it is possible to rollback the transaction manually if needed
    fn begin_transaction(
        &mut self,
    ) -> impl std::future::Future<Output = QueryResult<Self::Transaction<'_>>> + Send;

    /// Executes the given function inside of a database transaction
    ///
    /// This function executes the provided closure `f` inside a database
    /// transaction. The transaction is committed if
    /// the closure returns `Ok(_)`, it will be rolled back if it returns `Err(_)`.
    /// For both cases the original result value will be returned from this function.
    ///
    /// Automatic rollback is provided by underlying implementation of `AsyncTransaction`
    /// and does not leave a dangling transaction.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use diesel::result::Error;
    /// use benzin::{RunQueryDsl, AsyncConnection, AsyncTransactional};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let mut conn = TestConnection::default();
    /// conn.transaction(|transaction| Box::pin(async move {
    /// # transaction.expected_return_count(1);
    ///     diesel::insert_into(users)
    ///         .values(name.eq("Ruby"))
    ///         .execute(transaction)
    ///         .await?;
    ///
    /// # transaction.expected_load(vec![vec![String::from("Sean")],
    /// #                                vec![String::from("Tess")],
    /// #                                vec![String::from("Ruby")]]);
    ///     let all_names = users.select(name).load::<String>(transaction).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    ///     Ok::<(), Error>(())
    /// })).await;
    ///
    /// conn.transaction(|transaction| Box::pin(async move {
    /// # transaction.expected_return_count(1);
    ///     diesel::insert_into(users)
    ///         .values(name.eq("Pascal"))
    ///         .execute(transaction)
    ///         .await?;
    ///
    /// # transaction.expected_load(vec![vec![String::from("Sean")],
    /// #                                vec![String::from("Tess")],
    /// #                                vec![String::from("Ruby")],
    /// #                                vec![String::from("Pascal")]]);
    ///     let all_names = users.select(name).load::<String>(transaction).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby", "Pascal"], all_names);
    ///
    ///     // If we want to roll back the transaction, but don't have an
    ///     // actual error to return, we can return `RollbackTransaction`.
    ///     Err::<(), Error>(Error::RollbackTransaction)
    /// })).await;
    ///
    /// # conn.expected_load(vec![vec![String::from("Sean")],
    /// #                                vec![String::from("Tess")],
    /// #                                vec![String::from("Ruby")]]);
    /// let all_names = users.select(name).load::<String>(&mut conn).await?;
    /// assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// It is possible to nest transactions (if underlying implementation supports it)
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use diesel::result::Error;
    /// use benzin::{RunQueryDsl, AsyncConnection, AsyncTransactional};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let mut conn = TestConnection::default();
    /// conn.transaction(|transaction| Box::pin(async move {
    ///
    ///     let res =transaction.transaction(|inner_transaction| Box::pin(async move {
    /// # inner_transaction.expected_return_count(1);
    ///         diesel::insert_into(users)
    ///             .values(name.eq("Ruby"))
    ///             .execute(inner_transaction)
    ///             .await?;
    ///         Ok::<(), Error>(())})).await;
    ///     assert_eq!(res, Ok(()));
    /// # transaction.expected_load(vec![vec![String::from("Sean")],
    /// #                                vec![String::from("Tess")],
    /// #                                vec![String::from("Ruby")]]);
    ///     let all_names = users.select(name).load::<String>(transaction).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    ///
    ///     let res = transaction.transaction(|inner_transaction| Box::pin(async move {
    /// # inner_transaction.expected_return_count(1);
    ///         diesel::insert_into(users)
    ///             .values(name.eq("Pascal"))
    ///             .execute(inner_transaction)
    ///             .await?;
    ///         Err::<(), Error>(Error::RollbackTransaction)})).await;
    ///     assert_eq!(res, Err(Error::RollbackTransaction));
    /// # transaction.expected_load(vec![vec![String::from("Sean")],
    /// #                                vec![String::from("Tess")],
    /// #                                vec![String::from("Ruby")]]);
    ///     let all_names = users.select(name).load::<String>(transaction).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    ///     Err::<(), Error>(Error::RollbackTransaction)
    /// })).await;
    ///
    /// # conn.expected_load(vec![vec![String::from("Sean")],
    /// #                         vec![String::from("Tess")]]);
    /// let all_names = users.select(name).load::<String>(&mut conn).await?;
    /// assert_eq!(vec!["Sean", "Tess"], all_names);
    /// #     Ok(())
    /// # }
    /// ```
    fn transaction<'a, R, E, F>(
        &'a mut self,
        callback: F,
    ) -> impl std::future::Future<Output = Result<R, E>>
    where
        F: for<'t> FnOnce(
            &'t mut Self::Transaction<'a>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<R, E>> + 't>>,
        E: From<diesel::result::Error>,
    {
        async {
            let mut transaction = self.begin_transaction().await?;
            let res = callback(&mut transaction).await?;
            transaction.commit().await?;
            Ok(res)
        }
    }
}
