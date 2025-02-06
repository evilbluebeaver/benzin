use diesel::{
    query_builder::{AsQuery, QueryFragment, QueryId},
    QueryResult,
};
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use tokio_postgres::{
    types::{BorrowToSql, ToSql, Type},
    Error, Row, RowStream, Statement, ToStatement, Transaction,
};

use super::{
    cache::PgCache, error_helper::ErrorHelper, prepared_client::PreparedClient, row::PgRow,
};
use crate::{AsyncExecute, AsyncTransaction, AsyncTransactional};

/// Implementation of `AsyncTransaction` for `tokio_postgres::Transaction`
pub struct AsyncPgTransaction<'a> {
    transaction: tokio_postgres::Transaction<'a>,
    cache: &'a mut PgCache,
}

impl<'a> AsyncPgTransaction<'a> {
    pub(super) fn new(
        transaction: tokio_postgres::Transaction<'a>,
        cache: &'a mut PgCache,
    ) -> Self {
        Self { transaction, cache }
    }
}
impl AsyncTransaction for AsyncPgTransaction<'_> {
    async fn commit(self) -> QueryResult<()> {
        Ok(self.transaction.commit().await.map_err(ErrorHelper)?)
    }
    async fn rollback(self) -> QueryResult<()> {
        Ok(self.transaction.rollback().await.map_err(ErrorHelper)?)
    }
}

impl AsyncTransactional for AsyncPgTransaction<'_> {
    type Transaction<'a>
        = AsyncPgTransaction<'a>
    where
        Self: 'a;
    async fn begin_transaction(&mut self) -> QueryResult<Self::Transaction<'_>> {
        let transaction = self.transaction.transaction().await.map_err(ErrorHelper)?;
        let transaction = AsyncPgTransaction::new(transaction, self.cache);
        Ok(transaction)
    }
}

impl AsyncExecute for AsyncPgTransaction<'_> {
    type Stream<'conn>
        = BoxStream<'conn, QueryResult<PgRow>>
    where
        Self: 'conn;
    type Row<'conn>
        = PgRow
    where
        Self: 'conn;
    type Backend = diesel::pg::Pg;

    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        Ok(self
            .transaction
            .batch_execute(query)
            .await
            .map_err(ErrorHelper)?)
    }

    async fn load<T>(&mut self, source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
    {
        let res = self
            .cache
            .load_cached(&mut self.transaction, source)
            .await?;
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
            .execute_returning_count_cached(&mut self.transaction, source)
            .await
    }
}

impl PreparedClient for Transaction<'_> {
    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + Send + Sync + ToStatement,
    {
        (self as &Transaction<'_>)
            .query_one(statement, params)
            .await
    }
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        (self as &Transaction)
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
        (self as &Transaction<'_>)
            .query_raw(statement, params)
            .await
    }

    async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        (self as &Transaction<'_>).execute(statement, params).await
    }
}
