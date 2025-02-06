use std::{collections::HashMap, hash::Hash};

use diesel::{backend::Backend, connection::statement_cache::StatementCacheKey, QueryResult};

#[derive(Default)]
pub struct StmtCache<DB: Backend, P> {
    cache: HashMap<StatementCacheKey<DB>, P>,
}

pub enum CachedStatement<P, R> {
    /// Contains a reference cached prepared statement
    Prepared(P),
    /// Contains a not cached prepared statement
    Raw(R),
}
pub trait PrepareCallback<P, R, M>: Sized {
    async fn prepare(&mut self, sql: &str, metadata: &[M]) -> QueryResult<P>;
    async fn raw(&mut self, sql: &str, metadata: &[M]) -> QueryResult<R>;
}

impl<P, DB: Backend> StmtCache<DB, P> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub async fn cached_prepared_statement<F, R>(
        &mut self,
        cache_key: StatementCacheKey<DB>,
        sql: String,
        is_query_safe_to_cache: bool,
        metadata: &[DB::TypeMetadata],
        mut prepare_fn: F,
    ) -> QueryResult<CachedStatement<P, R>>
    where
        P: Send + Clone,
        R: Send + Clone,
        DB::QueryBuilder: Default,
        DB::TypeMetadata: Clone + Send + Sync,
        F: PrepareCallback<P, R, DB::TypeMetadata> + Send,
        StatementCacheKey<DB>: Hash + Eq,
    {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        if !is_query_safe_to_cache {
            return Ok(CachedStatement::Raw(prepare_fn.raw(&sql, metadata).await?));
        }

        match self.cache.entry(cache_key) {
            Occupied(entry) => {
                let stmt = entry.into_mut();
                Ok(CachedStatement::Prepared(stmt.clone()))
            }
            Vacant(entry) => {
                let metadata = metadata.to_vec();
                let stmt = prepare_fn.prepare(&sql, &metadata).await?;
                let stmt = entry.insert(stmt);
                Ok(CachedStatement::Prepared(stmt.clone()))
            }
        }
    }
}
