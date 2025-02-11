//! Provides types and functions related to working with MySQL
//!
//! Much of this module is re-exported from database agnostic locations.
//! However, if you are writing code specifically to extend Diesel on
//! MySQL, you may need to work with this module directly.
//!

use cache::MysqlCache;
use diesel::{
    associations::HasTable,
    mysql::Mysql,
    query_builder::{AsQuery, IntoUpdateTarget, QueryFragment, QueryId},
    result::{ConnectionError, ConnectionResult},
    AsChangeset, QueryResult,
};
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use mysql_async::{
    prelude::{Query, Queryable, WithParams},
    Opts, OptsBuilder, TxOpts,
};
pub use transaction::AsyncMysqlTransaction;

use crate::{
    stmt_cache::CachedStatement, AsyncConnection, AsyncExecute, AsyncTransactional, ExecuteDsl,
    LoadQuery, RunQueryDsl, UpdateAndFetchResults,
};

mod cache;
mod error_helper;
mod row;
mod serialize;
mod transaction;

use self::{error_helper::ErrorHelper, row::MysqlRow, serialize::ToSqlHelper};

/// A connection to a MySQL database.
///
/// Connection URLs should be in the form
/// `mysql://[user[:password]@]host/database_name`
///
/// Checkout the documentation of the [mysql_async]
/// crate for details about the format
///
pub struct AsyncMysqlConnection {
    conn: mysql_async::Conn,
    cache: MysqlCache,
    is_broken: bool,
}

impl AsyncExecute for AsyncMysqlConnection {
    type Stream<'conn> = BoxStream<'conn, QueryResult<MysqlRow>>;
    type Row<'conn> = MysqlRow;
    type Backend = Mysql;

    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        Ok(self
            .conn
            .query_drop(query)
            .await
            // TODO try to do this better in all the places
            .inspect_err(|e| {
                self.is_broken = e.is_fatal();
            })
            .map_err(ErrorHelper)?)
    }

    async fn load<T>(&mut self, source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
    {
        let (stmt, binds) = self
            .cache
            .with_prepared_statement(&mut self.conn, source.as_query())
            .await?;
        match stmt {
            CachedStatement::Prepared(stmt) => {
                let stream = self
                    .conn
                    .exec_stream(stmt, binds)
                    .await
                    .inspect_err(|e| {
                        self.is_broken = e.is_fatal();
                    })
                    .map_err(ErrorHelper)?
                    .inspect_err(|e| {
                        self.is_broken = e.is_fatal();
                    })
                    .map_err(|e| diesel::result::Error::from(ErrorHelper(e)));
                Ok(stream.boxed())
            }
            CachedStatement::Raw(query) => {
                let stream = query
                    .with(binds)
                    .stream(&mut self.conn)
                    .await
                    .inspect_err(|e| {
                        self.is_broken = e.is_fatal();
                    })
                    .map_err(ErrorHelper)?
                    .inspect_err(|e| {
                        self.is_broken = e.is_fatal();
                    })
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
            .with_prepared_statement(&mut self.conn, source)
            .await?;
        match stmt {
            CachedStatement::Prepared(stmt) => {
                self.conn
                    .exec_drop(stmt, binds)
                    .await
                    .inspect_err(|e| {
                        self.is_broken = e.is_fatal();
                    })
                    .map_err(ErrorHelper)?;
            }
            CachedStatement::Raw(query) => query
                .with(binds)
                .ignore(&mut self.conn)
                .await
                .inspect_err(|e| {
                    self.is_broken = e.is_fatal();
                })
                .map_err(ErrorHelper)?,
        }
        Ok(self.conn.affected_rows() as usize)
    }
}

const CONNECTION_SETUP_QUERIES: &[&str] = &[
    "SET time_zone = '+00:00';",
    "SET character_set_client = 'utf8mb4'",
    "SET character_set_connection = 'utf8mb4'",
    "SET character_set_results = 'utf8mb4'",
];

impl AsyncConnection for AsyncMysqlConnection {
    async fn establish(database_url: &str) -> diesel::ConnectionResult<Self> {
        let opts = Opts::from_url(database_url)
            .map_err(|e| diesel::result::ConnectionError::InvalidConnectionUrl(e.to_string()))?;
        let builder = OptsBuilder::from_opts(opts)
            .init(CONNECTION_SETUP_QUERIES.to_vec())
            .stmt_cache_size(0); // We have our own cache

        let conn = mysql_async::Conn::new(builder).await.map_err(ErrorHelper)?;

        Ok(AsyncMysqlConnection {
            conn,
            cache: MysqlCache::new(),
            is_broken: false,
        })
    }
    fn is_broken(&self) -> bool {
        self.is_broken
    }
}

impl AsyncTransactional for AsyncMysqlConnection {
    type Transaction<'a>
        = AsyncMysqlTransaction<'a>
    where
        Self: 'a;
    async fn begin_transaction(&mut self) -> QueryResult<Self::Transaction<'_>> {
        let transaction = self
            .conn
            .start_transaction(TxOpts::default())
            .await
            .inspect_err(|e| {
                self.is_broken = e.is_fatal();
            })
            .map_err(ErrorHelper)?;
        let cache = &mut self.cache;
        Ok(AsyncMysqlTransaction {
            transaction,
            cache,
            is_broken: &mut self.is_broken,
        })
    }
}

impl AsyncMysqlConnection {
    /// Wrap an existing [`mysql_async::Conn`] into a async diesel mysql connection
    ///
    /// This function constructs a new `AsyncMysqlConnection` based on an existing
    /// [`mysql_async::Conn]`.
    pub async fn try_from(conn: mysql_async::Conn) -> ConnectionResult<Self> {
        use crate::run_query_dsl::RunQueryDsl;
        let mut conn = AsyncMysqlConnection {
            conn,
            cache: MysqlCache::new(),
            is_broken: false,
        };

        for stmt in CONNECTION_SETUP_QUERIES {
            diesel::sql_query(*stmt)
                .execute(&mut conn)
                .await
                .map_err(ConnectionError::CouldntSetupConfiguration)?;
        }

        Ok(conn)
    }
}

impl<Changes, Output> UpdateAndFetchResults<Changes, Output> for crate::AsyncMysqlConnection
where
    Output: Send,
    Changes: Copy + diesel::Identifiable + Send,
    Changes: AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget,
    Changes::Table: diesel::query_dsl::methods::FindDsl<Changes::Id> + Send,
    Changes::WhereClause: Send,
    Changes::Changeset: Send,
    Changes::Id: Send,
    diesel::dsl::Update<Changes, Changes>: ExecuteDsl<crate::AsyncMysqlConnection>,
    diesel::dsl::Find<Changes::Table, Changes::Id>:
        LoadQuery<crate::AsyncMysqlConnection, Output> + Send,
    <Changes::Table as diesel::Table>::AllColumns: diesel::expression::ValidGrouping<()>,
    <<Changes::Table as diesel::Table>::AllColumns as diesel::expression::ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
        diesel::expression::is_aggregate::No,
        Output = diesel::expression::is_aggregate::No,
    >,
    <Changes::Table as diesel::query_source::QuerySource>::FromClause: Send,
{
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    {
        use diesel::query_dsl::methods::FindDsl;

        diesel::update(changeset)
            .set(changeset)
            .execute(self)
            .await?;
        Changes::table().find(changeset.id()).get_result(self).await
    }
}
