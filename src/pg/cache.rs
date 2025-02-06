use diesel::{
    connection::statement_cache::StatementCacheKey,
    pg::{Pg, PgMetadataCache, PgMetadataCacheKey, PgQueryBuilder, PgTypeMetadata},
    query_builder::{
        bind_collector::RawBytesBindCollector, AsQuery, QueryBuilder, QueryFragment, QueryId,
    },
    QueryResult,
};
use tokio_postgres::{types::ToSql, RowStream, Statement};

use super::{
    error_helper::ErrorHelper, metadata::PgAsyncMetadataLookup, prepared_client::PreparedClient,
    serialize::ToSqlHelper,
};
use crate::stmt_cache::{CachedStatement, StmtCache};

pub struct PgCache {
    stmt_cache: StmtCache<Pg, tokio_postgres::Statement>,
    metadata_cache: PgMetadataCache,
}

impl PgCache {
    pub fn new() -> Self {
        Self {
            stmt_cache: StmtCache::new(),
            metadata_cache: PgMetadataCache::new(),
        }
    }
    pub async fn load_cached<'conn, T, W>(
        &'conn mut self,
        prepared_client: &'conn mut W,
        source: T,
    ) -> QueryResult<RowStream>
    where
        T: AsQuery,
        T::Query: QueryFragment<Pg> + QueryId,
        W: PreparedClient + Send + Sync + 'conn,
    {
        let query = source.as_query();
        let (stmt, binds) = self.get_statement(prepared_client, query).await?;
        let stmt = match stmt {
            CachedStatement::Prepared(stmt) => stmt,
            CachedStatement::Raw(stmt) => stmt,
        };
        Ok(prepared_client
            .query_raw(&stmt, binds)
            .await
            .map_err(ErrorHelper)?)
    }

    pub async fn execute_returning_count_cached<'conn, T, W>(
        &'conn mut self,
        prepared_client: &'conn mut W,
        source: T,
    ) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId + Send,
        W: PreparedClient + Send + Sync + 'conn,
    {
        let (stmt, binds) = self.get_statement(prepared_client, source).await?;
        let stmt = match stmt {
            CachedStatement::Prepared(stmt) => stmt,
            CachedStatement::Raw(stmt) => stmt,
        };

        let binds = binds
            .iter()
            .map(|b| b as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let res = prepared_client
            .execute(&stmt, &binds)
            .await
            .map_err(ErrorHelper)?;
        Ok(res as usize)
    }
    async fn get_statement<'conn, T, W>(
        &'conn mut self,
        prepared_client: &'conn W,
        query: T,
    ) -> QueryResult<(CachedStatement<Statement, Statement>, Vec<ToSqlHelper>)>
    where
        T: QueryFragment<Pg> + QueryId,
        W: PreparedClient + Send + Sync + 'conn,
    {
        // we explicilty descruct the query here before going into the async block
        //
        // That's required to remove the send bound from `T` as we have translated
        // the query type to just a string (for the SQL) and a bunch of bytes (for the binds)
        // which both are `Send`.
        // We also collect the query id (essentially an integer) and the safe_to_cache flag here
        // so there is no need to even access the query in the async block below
        let is_safe_to_cache_prepared = query.is_safe_to_cache_prepared(&Pg)?;
        let mut query_builder = PgQueryBuilder::default();
        let sql = query
            .to_sql(&mut query_builder, &Pg)
            .map(|_| query_builder.finish())?;

        let mut bind_collector = RawBytesBindCollector::<Pg>::new();
        let query_id = T::query_id();

        // we don't resolve custom types here yet, we do that later
        // in the async block below as we might need to perform lookup
        // queries for that.
        //
        // We apply this workaround to prevent requiring all the diesel
        // serialization code to beeing async
        let mut metadata_lookup = PgAsyncMetadataLookup::new();
        query.collect_binds(&mut bind_collector, &mut metadata_lookup, &Pg)?;
        let stmt_cache = &mut self.stmt_cache;
        let metadata_cache = &mut self.metadata_cache;

        // Check whether we need to resolve some types at all
        //
        // If the user doesn't use custom types there is no need
        // to borther with that at all
        if !metadata_lookup.unresolved_types.is_empty() {
            let mut next_unresolved = metadata_lookup.unresolved_types.into_iter();
            for m in &mut bind_collector.metadata {
                // for each unresolved item
                // we check whether it's arleady in the cache
                // or perform a lookup and insert it into the cache
                if m.oid().is_err() {
                    if let Some((ref schema, ref lookup_type_name)) = next_unresolved.next() {
                        let cache_key = PgMetadataCacheKey::new(
                            schema.as_ref().map(Into::into),
                            lookup_type_name.into(),
                        );
                        if let Some(entry) = metadata_cache.lookup_type(&cache_key) {
                            *m = entry;
                        } else {
                            let type_metadata = lookup_type(
                                schema.clone(),
                                lookup_type_name.clone(),
                                prepared_client,
                            )
                            .await?;
                            *m = PgTypeMetadata::from_result(Ok(type_metadata));

                            metadata_cache.store_type(cache_key, type_metadata);
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        let key = match query_id {
            Some(id) => StatementCacheKey::Type(id),
            None => StatementCacheKey::Sql {
                sql: sql.clone(),
                bind_types: bind_collector.metadata.clone(),
            },
        };
        let stmt = {
            stmt_cache
                .cached_prepared_statement(
                    key,
                    sql,
                    is_safe_to_cache_prepared,
                    &bind_collector.metadata,
                    prepared_client,
                )
                .await?
        };
        let binds = bind_collector
            .metadata
            .into_iter()
            .zip(bind_collector.binds)
            .map(|(meta, bind)| ToSqlHelper(meta, bind))
            .collect::<Vec<_>>();

        Ok((stmt, binds))
    }
}

async fn lookup_type<W: PreparedClient>(
    schema: Option<String>,
    type_name: String,
    prepared_client: &W,
) -> QueryResult<(u32, u32)> {
    let r = if let Some(schema) = schema.as_ref() {
        prepared_client
            .query_one(
                "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
             INNER JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid \
             WHERE pg_type.typname = $1 AND pg_namespace.nspname = $2 \
             LIMIT 1",
                &[&type_name, schema],
            )
            .await
            .map_err(ErrorHelper)?
    } else {
        prepared_client
            .query_one(
                "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
             WHERE pg_type.oid = quote_ident($1)::regtype::oid \
             LIMIT 1",
                &[&type_name],
            )
            .await
            .map_err(ErrorHelper)?
    };
    Ok((r.get(0), r.get(1)))
}
