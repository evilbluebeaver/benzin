use diesel::{
    connection::statement_cache::StatementCacheKey,
    mysql::{Mysql, MysqlQueryBuilder, MysqlType},
    query_builder::{bind_collector::RawBytesBindCollector, QueryBuilder, QueryFragment, QueryId},
    QueryResult,
};
use mysql_async::{prelude::Queryable, Statement};

use super::{ErrorHelper, ToSqlHelper};
use crate::stmt_cache::{CachedStatement, PrepareCallback, StmtCache};

pub struct MysqlCache {
    stmt_cache: StmtCache<Mysql, mysql_async::Statement>,
}

impl MysqlCache {
    pub fn new() -> Self {
        Self {
            stmt_cache: StmtCache::new(),
        }
    }

    pub(super) async fn with_prepared_statement<'a, T>(
        &'a mut self,
        conn: &'a mut impl Queryable,
        query: T,
    ) -> QueryResult<(CachedStatement<Statement, String>, ToSqlHelper)>
    where
        T: QueryFragment<Mysql> + QueryId,
    {
        let query_id = T::query_id();
        let mut bind_collector = RawBytesBindCollector::<Mysql>::new();
        let bind_collector = query
            .collect_binds(&mut bind_collector, &mut (), &Mysql)
            .map(|()| bind_collector);

        let is_safe_to_cache_prepared = query.is_safe_to_cache_prepared(&Mysql)?;
        let mut qb = MysqlQueryBuilder::new();
        let sql = query.to_sql(&mut qb, &Mysql).map(|()| qb.finish())?;

        let RawBytesBindCollector {
            metadata, binds, ..
        } = bind_collector?;
        let cache_key = match query_id {
            Some(query_id) => StatementCacheKey::Type(query_id),
            None => StatementCacheKey::Sql {
                sql: sql.clone(),
                bind_types: metadata.clone(),
            },
        };

        let stmt = self
            .stmt_cache
            .cached_prepared_statement(cache_key, sql, is_safe_to_cache_prepared, &metadata, conn)
            .await?;

        Ok((stmt, ToSqlHelper { metadata, binds }))
    }
}

impl<T> PrepareCallback<Statement, String, MysqlType> for &mut T
where
    T: Queryable,
{
    async fn prepare(&mut self, sql: &str, _metadata: &[MysqlType]) -> QueryResult<Statement> {
        let s = self.prep(sql).await.map_err(ErrorHelper)?;
        Ok(s)
    }
    async fn raw(&mut self, sql: &str, _metadata: &[MysqlType]) -> QueryResult<String> {
        Ok(sql.to_string())
    }
}
