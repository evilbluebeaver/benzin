use diesel::{
    associations::HasTable, query_builder::IntoUpdateTarget, result::QueryResult, AsChangeset,
};
use futures_util::{pin_mut, Future, Stream, StreamExt, TryFutureExt, TryStreamExt};
pub use methods::{ExecuteDsl, LoadQuery};

use crate::AsyncExecute;

/// The traits used by `QueryDsl`.
///
/// Each trait in this module represents exactly one method from [`RunQueryDsl`].
/// Apps should general rely on [`RunQueryDsl`] directly, rather than these traits.
/// However, generic code may need to include a where clause that references
/// these traits.
pub mod methods {

    use diesel::{
        backend::Backend,
        deserialize::FromSqlRow,
        expression::QueryMetadata,
        query_builder::{AsQuery, QueryFragment, QueryId},
        query_dsl::CompatibleType,
    };
    use futures_util::{Stream, StreamExt};

    use super::*;
    use crate::AsyncExecute;

    /// The `execute` method
    ///
    /// This trait should not be relied on directly by most apps. Its behavior is
    /// provided by [`RunQueryDsl`]. However, you may need a where clause on this trait
    /// to call `execute` from generic code.
    ///
    /// [`RunQueryDsl`]: super::RunQueryDsl
    pub trait ExecuteDsl<Conn, DB = <Conn as AsyncExecute>::Backend>
    where
        Conn: AsyncExecute<Backend = DB>,
        DB: Backend,
    {
        /// Execute this command
        fn execute(query: Self, conn: &mut Conn)
            -> impl Future<Output = QueryResult<usize>> + Send;
    }

    impl<Conn, DB, T> ExecuteDsl<Conn, DB> for T
    where
        Conn: AsyncExecute<Backend = DB>,
        DB: Backend,
        T: QueryFragment<DB> + QueryId + Send,
    {
        async fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
            conn.execute_returning_count(query).await
        }
    }

    /// The `load` method
    ///
    /// This trait should not be relied on directly by most apps. Its behavior is
    /// provided by [`RunQueryDsl`]. However, you may need a where clause on this trait
    /// to call `load` from generic code.
    ///
    /// [`RunQueryDsl`]: super::RunQueryDsl
    pub trait LoadQuery<Conn: AsyncExecute, U> {
        /// Load this query
        fn internal_load(
            self,
            conn: &mut Conn,
        ) -> impl Future<Output = QueryResult<impl Stream<Item = QueryResult<U>> + Send>> + Send;
    }

    impl<Conn, DB, T, U, ST> LoadQuery<Conn, U> for T
    where
        Conn: AsyncExecute<Backend = DB>,
        U: FromSqlRow<ST, DB> + Send,
        DB: Backend + 'static,
        T: AsQuery + Send,
        T::Query: QueryFragment<DB> + QueryId + Send,
        T::SqlType: CompatibleType<U, DB, SqlType = ST>,
        U: FromSqlRow<ST, DB> + Send + 'static,
        DB: QueryMetadata<T::SqlType>,
        ST: 'static,
    {
        async fn internal_load(
            self,
            conn: &mut Conn,
        ) -> QueryResult<impl Stream<Item = QueryResult<U>>> {
            let result = conn.load(self);
            result
                .map_ok(|stream| {
                    stream.map(|row| {
                        U::build_from_row(&row?)
                            .map_err(diesel::result::Error::DeserializationError)
                    })
                })
                .await
        }
    }
}

/// Methods used to execute queries.
pub trait RunQueryDsl<Conn>: Sized + Send {
    /// Executes the given command, returning the number of rows affected.
    ///
    /// `execute` is usually used in conjunction with [`insert_into`](diesel::insert_into()),
    /// [`update`](diesel::update()) and [`delete`](diesel::delete()) where the number of
    /// affected rows is often enough information.
    ///
    /// When asking the database to return data from a query, [`load`](crate::run_query_dsl::RunQueryDsl::load()) should
    /// probably be used instead.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// #
    /// use benzin::RunQueryDsl;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_return_count(1);
    /// let inserted_rows = insert_into(users)
    ///     .values(name.eq("Ruby"))
    ///     .execute(connection)
    ///     .await?;
    /// assert_eq!(1, inserted_rows);
    ///
    /// # connection.expected_return_count(2);
    /// let inserted_rows = insert_into(users)
    ///     .values(&vec![name.eq("Jim"), name.eq("James")])
    ///     .execute(connection)
    ///     .await?;
    /// assert_eq!(2, inserted_rows);
    /// #     Ok(())
    /// # }
    /// ```
    fn execute(self, conn: &mut Conn) -> impl Future<Output = QueryResult<usize>> + Send
    where
        Conn: AsyncExecute + Send,
        Self: methods::ExecuteDsl<Conn>,
    {
        methods::ExecuteDsl::execute(self, conn)
    }

    /// Executes the given query, returning a [`Vec`] with the returned rows.
    ///
    /// When using the query builder, the return type can be
    /// a tuple of the values, or a struct which implements [`Queryable`].
    ///
    /// When this method is called on [`sql_query`],
    /// the return type can only be a struct which implements [`QueryableByName`]
    ///
    /// For insert, update, and delete operations where only a count of affected is needed,
    /// [`execute`] should be used instead.
    ///
    /// [`Queryable`]: diesel::deserialize::Queryable
    /// [`QueryableByName`]: diesel::deserialize::QueryableByName
    /// [`execute`]: crate::run_query_dsl::RunQueryDsl::execute()
    /// [`sql_query`]: diesel::sql_query()
    ///
    /// # Examples
    ///
    /// ## Returning a single field
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// #
    /// use benzin::{RunQueryDsl, AsyncConnection};
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("Sean")],
    /// #                               vec![String::from("Tess")]]);
    /// let data = users.select(name)
    ///     .load::<String>(connection)
    ///     .await?;
    /// assert_eq!(vec!["Sean", "Tess"], data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a tuple
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use benzin::RunQueryDsl;
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("1"), String::from("Sean")],
    /// #                               vec![String::from("2"), String::from("Tess")]]);
    /// let data = users
    ///     .load::<(i32, String)>(connection)
    ///     .await?;
    /// let expected_data = vec![
    ///     (1, String::from("Sean")),
    ///     (2, String::from("Tess")),
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a struct
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use benzin::RunQueryDsl;
    ///
    /// #
    /// #[derive(Queryable, PartialEq, Debug)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("1"), String::from("Sean")],
    /// #                               vec![String::from("2"), String::from("Tess")]]);
    /// let data = users
    ///     .load::<User>(connection)
    ///     .await?;
    /// let expected_data = vec![
    ///     User { id: 1, name: String::from("Sean") },
    ///     User { id: 2, name: String::from("Tess") },
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    fn load<U>(self, conn: &mut Conn) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: Send,
        Conn: AsyncExecute,
        Self: methods::LoadQuery<Conn, U>,
    {
        self.load_stream(conn).and_then(TryStreamExt::try_collect)
    }

    /// Executes the given query, returning a [`Stream`] with the returned rows.
    ///
    /// **You should normally prefer to use [`RunQueryDsl::load`] instead**. This method
    /// is provided for situations where the result needs to be collected into a different
    /// container than a [`Vec`]
    ///
    /// When using the query builder, the return type can be
    /// a tuple of the values, or a struct which implements [`Queryable`].
    ///
    /// When this method is called on [`sql_query`],
    /// the return type can only be a struct which implements [`QueryableByName`]
    ///
    /// For insert, update, and delete operations where only a count of affected is needed,
    /// [`execute`] should be used instead.
    ///
    /// [`Queryable`]: diesel::deserialize::Queryable
    /// [`QueryableByName`]: diesel::deserialize::QueryableByName
    /// [`execute`]: crate::run_query_dsl::RunQueryDsl::execute()
    /// [`sql_query`]: diesel::sql_query()
    ///
    /// # Examples
    ///
    /// ## Returning a single field
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// #
    /// use benzin::RunQueryDsl;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("Sean")],
    /// #                               vec![String::from("Tess")]]);
    /// let data = users.select(name)
    ///     .load_stream::<String>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// assert_eq!(vec!["Sean", "Tess"], data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a tuple
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use benzin::RunQueryDsl;
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("1"), String::from("Sean")],
    /// #                               vec![String::from("2"), String::from("Tess")]]);
    /// let data = users
    ///     .load_stream::<(i32, String)>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// let expected_data = vec![
    ///     (1, String::from("Sean")),
    ///     (2, String::from("Tess")),
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a struct
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// #
    /// use benzin::RunQueryDsl;
    ///
    /// #[derive(Queryable, PartialEq, Debug)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("1"), String::from("Sean")],
    /// #                               vec![String::from("2"), String::from("Tess")]]);
    /// let data = users
    ///     .load_stream::<User>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// let expected_data = vec![
    ///     User { id: 1, name: String::from("Sean") },
    ///     User { id: 2, name: String::from("Tess") },
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    fn load_stream<U>(
        self,
        conn: &mut Conn,
    ) -> impl Future<Output = QueryResult<impl Stream<Item = QueryResult<U>> + Send>> + Send
    where
        Conn: AsyncExecute,
        Self: methods::LoadQuery<Conn, U>,
    {
        self.internal_load(conn)
    }

    /// Runs the command, and returns the affected row.
    ///
    /// `Err(NotFound)` will be returned if the query affected 0 rows. You can
    /// call `.optional()` on the result of this if the command was optional to
    /// get back a `Result<Option<U>>`
    ///
    /// When this method is called on an insert, update, or delete statement,
    /// it will implicitly add a `RETURNING *` to the query,
    /// unless a returning clause was already specified.
    ///
    /// This method only returns the first row that was affected, even if more
    /// rows are affected.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use benzin::RunQueryDsl;
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::{insert_into, update};
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    /// # connection.expected_load(vec![vec![String::from("3"), String::from("Ruby")]]);
    /// let inserted_row = insert_into(users)
    ///     .values(name.eq("Ruby"))
    ///     .get_result(connection)
    ///     .await?;
    /// assert_eq!((3, String::from("Ruby")), inserted_row);
    ///
    /// // This will return `NotFound`, as there is no user with ID 4
    /// let update_result = update(users.find(4))
    ///     .set(name.eq("Jim"))
    ///     .get_result::<(i32, String)>(connection)
    ///     .await;
    /// assert_eq!(Err(diesel::NotFound), update_result);
    /// #     Ok(())
    /// # }
    /// #
    /// ```
    fn get_result<U>(self, conn: &mut Conn) -> impl Future<Output = QueryResult<U>> + Send
    where
        U: Send,
        Conn: AsyncExecute,
        Self: methods::LoadQuery<Conn, U>,
    {
        async {
            let stream = self.load_stream(conn).await?;
            pin_mut!(stream);
            stream
                .next()
                .await
                .unwrap_or(Err(diesel::result::Error::NotFound))
        }
    }

    /// Runs the command, returning an `Vec` with the affected rows.
    ///
    /// This method is an alias for [`load`], but with a name that makes more
    /// sense for insert, update, and delete statements.
    ///
    /// [`load`]: crate::run_query_dsl::RunQueryDsl::load()
    fn get_results<U>(self, conn: &mut Conn) -> impl Future<Output = QueryResult<Vec<U>>> + Send
    where
        U: Send,
        Conn: AsyncExecute,
        Self: methods::LoadQuery<Conn, U>,
    {
        self.load(conn)
    }

    /// Attempts to load a single record.
    ///
    /// This method is equivalent to `.limit(1).get_result()`
    ///
    /// Returns `Ok(record)` if found, and `Err(NotFound)` if no results are
    /// returned. If the query truly is optional, you can call `.optional()` on
    /// the result of this to get a `Result<Option<U>>`.
    ///
    /// # Example:
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use benzin::RunQueryDsl;
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut TestConnection::default();
    ///
    /// # connection.expected_return_count(2);
    /// diesel::insert_into(users)
    ///     .values(&vec![name.eq("Sean"), name.eq("Pascal")])
    ///     .execute(connection)
    ///     .await?;
    ///
    /// # connection.expected_load(vec![vec![String::from("Sean")]]);
    /// let first_name = users.order(id)
    ///     .select(name)
    ///     .first(connection)
    ///     .await;
    /// assert_eq!(Ok(String::from("Sean")), first_name);
    ///
    /// let not_found = users
    ///     .filter(name.eq("Foo"))
    ///     .first::<(i32, String)>(connection)
    ///     .await;
    /// assert_eq!(Err(diesel::NotFound), not_found);
    /// #     Ok(())
    /// # }
    /// ```
    fn first<U>(self, conn: &mut Conn) -> impl Future<Output = QueryResult<U>> + Send
    where
        U: Send,
        Conn: AsyncExecute,
        Self: diesel::query_dsl::methods::LimitDsl,
        diesel::dsl::Limit<Self>: methods::LoadQuery<Conn, U> + Send,
    {
        diesel::query_dsl::methods::LimitDsl::limit(self, 1).get_result(conn)
    }
}

impl<T: Send, Conn> RunQueryDsl<Conn> for T {}

/// Sugar for types which implement both `AsChangeset` and `Identifiable`
///
/// On backends which support the `RETURNING` keyword,
/// `foo.save_changes(&conn)` is equivalent to
/// `update(&foo).set(&foo).get_result(&conn)`.
/// On other backends, two queries will be executed.
///
/// # Example
///
/// ```rust
/// # include!("doctest_setup.rs");
/// # use schema::animals;
/// #
/// use benzin::{SaveChangesDsl, AsyncConnection};
///
/// #[derive(Queryable, Debug, PartialEq)]
/// struct Animal {
///    id: i32,
///    species: String,
///    legs: i32,
///    name: Option<String>,
/// }
///
/// #[derive(AsChangeset, Identifiable)]
/// #[diesel(table_name = animals)]
/// struct AnimalForm<'a> {
///     id: i32,
///     name: &'a str,
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run_test().await.unwrap();
/// # }
/// #
/// # async fn run_test() -> QueryResult<()> {
/// #     use self::animals::dsl::*;
/// #     let connection = &mut TestConnection::default();
/// let form = AnimalForm { id: 2, name: "Super scary" };
///
/// # connection.expected_load(vec![vec![String::from("2"), String::from("spider"),
/// #                                    String::from("8"), String::from("Super scary")]]);
/// let changed_animal = form.save_changes(connection).await?;
/// let expected_animal = Animal {
///     id: 2,
///     species: String::from("spider"),
///     legs: 8,
///     name: Some(String::from("Super scary")),
/// };
/// assert_eq!(expected_animal, changed_animal);
/// #     Ok(())
/// # }
/// ```
pub trait SaveChangesDsl<Conn> {
    /// See the trait documentation
    fn save_changes<T>(self, connection: &mut Conn) -> impl Future<Output = QueryResult<T>> + Send
    where
        Self: Sized + diesel::prelude::Identifiable,
        Conn: UpdateAndFetchResults<Self, T>,
    {
        connection.update_and_fetch(self)
    }
}

impl<T, Conn> SaveChangesDsl<Conn> for T where
    T: Copy + AsChangeset<Target = <T as HasTable>::Table> + IntoUpdateTarget
{
}

/// A trait defining how to update a record and fetch the updated entry
/// on a certain backend.
///
/// The only case where it is required to work with this trait is while
/// implementing a new connection type.
/// Otherwise use [`SaveChangesDsl`]
///
/// For implementing this trait for a custom backend:
/// * The `Changes` generic parameter represents the changeset that should be stored
/// * The `Output` generic parameter represents the type of the response.
pub trait UpdateAndFetchResults<Changes, Output>: AsyncExecute
where
    Changes: diesel::prelude::Identifiable + HasTable,
{
    /// See the traits documentation.
    fn update_and_fetch(
        &mut self,
        changeset: Changes,
    ) -> impl Future<Output = QueryResult<Output>> + Send;
}
