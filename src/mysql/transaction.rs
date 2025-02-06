use diesel::{
    mysql::Mysql,
    query_builder::{AsQuery, QueryFragment, QueryId},
    QueryResult,
};
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use mysql_async::prelude::{Query, Queryable};

use super::{cache::MysqlCache, row::MysqlRow, ErrorHelper};
use crate::{stmt_cache::CachedStatement, AsyncExecute, AsyncTransaction};

/// A transaction on a MySql database.
///
/// Does not support nested transactions.
pub struct AsyncMysqlTransaction<'a> {
    pub(super) transaction: mysql_async::Transaction<'a>,
    pub(super) cache: &'a mut MysqlCache,
}

impl AsyncExecute for AsyncMysqlTransaction<'_> {
    type Stream<'conn>
        = BoxStream<'conn, QueryResult<MysqlRow>>
    where
        Self: 'conn;
    type Row<'conn>
        = MysqlRow
    where
        Self: 'conn;
    type Backend = Mysql;

    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        let result = self.transaction.query_drop(query).await;
        Ok(result.map_err(ErrorHelper)?)
    }

    async fn load<T>(&mut self, source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
    {
        let (stmt, binds) = self
            .cache
            .with_prepared_statement(&mut self.transaction, source.as_query())
            .await?;
        match stmt {
            CachedStatement::Prepared(stmt) => {
                let stream = self
                    .transaction
                    .exec_stream(stmt, binds)
                    .await
                    .map_err(ErrorHelper)?
                    .map_err(|e| diesel::result::Error::from(ErrorHelper(e)));
                Ok(stream.boxed())
            }
            CachedStatement::Raw(query) => {
                let stream = query
                    .stream(&mut self.transaction)
                    .await
                    .map_err(ErrorHelper)?
                    .map_err(|e| diesel::result::Error::from(ErrorHelper(e)));
                Ok(stream.boxed())
            }
        }
    }

    async fn execute_returning_count<T>(&mut self, source: T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send,
    {
        let (stmt, binds) = self
            .cache
            .with_prepared_statement(&mut self.transaction, source)
            .await?;
        match stmt {
            CachedStatement::Prepared(stmt) => {
                self.transaction
                    .exec_drop(stmt, binds)
                    .await
                    .map_err(ErrorHelper)?;
            }
            CachedStatement::Raw(query) => query
                .ignore(&mut self.transaction)
                .await
                .map_err(ErrorHelper)?,
        }
        Ok(self.transaction.affected_rows() as usize)
    }
}
impl AsyncTransaction for AsyncMysqlTransaction<'_> {
    async fn commit(self) -> QueryResult<()> {
        Ok(self.transaction.commit().await.map_err(ErrorHelper)?)
    }

    async fn rollback(self) -> QueryResult<()> {
        Ok(self.transaction.rollback().await.map_err(ErrorHelper)?)
    }
}
