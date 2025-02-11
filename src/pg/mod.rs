//! Provides types and functions related to working with PostgreSQL
//!
//! Much of this module is re-exported from database agnostic locations.
//! However, if you are writing code specifically to extend Diesel on
//! PostgreSQL, you may need to work with this module directly.

use cache::PgCache;
use diesel::{
    query_builder::{AsQuery, IntoUpdateTarget, QueryFragment, QueryId},
    AsChangeset, ConnectionError, ConnectionResult, QueryResult,
};
use futures_util::stream::{BoxStream, StreamExt, TryStreamExt};
use prepared_client::PreparedClient;
use tokio_postgres::{
    types::{BorrowToSql, ToSql, Type},
    Client, Error, Row, RowStream, Statement, ToStatement,
};
pub use transaction::AsyncPgTransaction;

use self::{error_helper::ErrorHelper, row::PgRow};
use crate::{
    run_query_dsl::*, AsyncConnection, AsyncExecute, AsyncTransactional, UpdateAndFetchResults,
};

mod cache;
mod error_helper;
mod metadata;
mod prepared_client;
mod row;
mod serialize;
mod transaction;

/// A connection to a PostgreSQL database.
///
/// Connection URLs should be in the form
/// `postgres://[user[:password]@]host/database_name`
///
/// Checkout the documentation of the [tokio_postgres]
/// crate for details about the format
///
/// [tokio_postgres]: https://docs.rs/tokio-postgres/0.7.6/tokio_postgres/config/struct.Config.html#url
///
pub struct AsyncPgConnection {
    conn: tokio_postgres::Client,
    cache: PgCache,
}

/// A transaction on a PostgreSQL database.
///
/// Supports nested transactions.
impl AsyncExecute for AsyncPgConnection {
    type Stream<'conn> = BoxStream<'conn, QueryResult<PgRow>>;
    type Row<'conn> = PgRow;
    type Backend = diesel::pg::Pg;

    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        Ok(self.conn.batch_execute(query).await.map_err(ErrorHelper)?)
    }

    async fn load<T>(&mut self, source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
    {
        let res = self.cache.load_cached(&mut self.conn, source).await?;
        let res = res
            .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
            .map_ok(PgRow::new);
        Ok(res.boxed())
    }

    async fn execute_returning_count<T>(&mut self, source: T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send,
    {
        self.cache
            .execute_returning_count_cached(&mut self.conn, source)
            .await
    }
}

impl AsyncTransactional for AsyncPgConnection {
    type Transaction<'a>
        = AsyncPgTransaction<'a>
    where
        Self: 'a;
    async fn begin_transaction(&mut self) -> QueryResult<Self::Transaction<'_>> {
        let transaction = self.conn.transaction().await.map_err(ErrorHelper)?;
        let transaction = AsyncPgTransaction::new(transaction, &mut self.cache);
        Ok(transaction)
    }
}

impl AsyncConnection for AsyncPgConnection {
    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(ErrorHelper)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Self::setup(client).await
    }
    fn is_broken(&self) -> bool {
        self.conn.is_closed()
    }
}

impl AsyncPgConnection {
    /// Construct a new `AsyncPgConnection` instance from an existing [`tokio_postgres::Client`]
    pub async fn try_from(conn: tokio_postgres::Client) -> ConnectionResult<Self> {
        Self::setup(conn).await
    }

    async fn setup(conn: tokio_postgres::Client) -> ConnectionResult<Self> {
        let cache = PgCache::new();
        let mut result = Self { conn, cache };
        result
            .set_config_options()
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(result)
    }
    async fn set_config_options(&mut self) -> QueryResult<()> {
        use crate::run_query_dsl::RunQueryDsl;

        diesel::sql_query("SET TIME ZONE 'UTC'")
            .execute(self)
            .await?;
        diesel::sql_query("SET CLIENT_ENCODING TO 'UTF8'")
            .execute(self)
            .await?;
        Ok(())
    }

    /// Constructs a cancellation token that can later be used to request cancellation of a query running on the connection associated with this client.
    pub fn cancel_token(&self) -> tokio_postgres::CancelToken {
        self.conn.cancel_token()
    }
}

impl PreparedClient for Client {
    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + Send + Sync + ToStatement,
    {
        (self as &Client).query_one(statement, params).await
    }
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        (self as &Client)
            .prepare_typed(query, parameter_types)
            .await
    }
    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + Send + Sync + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Send + Sync,
        I::IntoIter: ExactSizeIterator,
    {
        (self as &Client).query_raw(statement, params).await
    }
    async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ToStatement + ?Sized,
    {
        (self as &Client).execute(statement, params).await
    }
}

impl<Changes, Output, Tab, V> UpdateAndFetchResults<Changes, Output> for crate::AsyncPgConnection
where
    Output: Send,
    Changes:
        Copy + AsChangeset<Target = Tab> + Send + diesel::associations::Identifiable<Table = Tab>,
    Tab: diesel::Table + diesel::query_dsl::methods::FindDsl<Changes::Id>,
    diesel::dsl::Find<Tab, Changes::Id>: IntoUpdateTarget<Table = Tab, WhereClause = V>,
    diesel::query_builder::UpdateStatement<Tab, V, Changes::Changeset>:
        diesel::query_builder::AsQuery,
    diesel::dsl::Update<Changes, Changes>: LoadQuery<Self, Output>,
    V: Send,
    Changes::Changeset: Send,
    Tab::FromClause: Send,
{
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output> {
        diesel::update(changeset)
            .set(changeset)
            .get_result(self)
            .await
    }
}
