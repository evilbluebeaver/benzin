use diesel::{pg::PgTypeMetadata, QueryResult};
use tokio_postgres::{
    types::{BorrowToSql, ToSql, Type},
    Error, Row, RowStream, Statement, ToStatement,
};

use super::error_helper::ErrorHelper;
use crate::stmt_cache::PrepareCallback;

pub trait PreparedClient
where
    Self: Sync,
{
    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + Send + Sync + ToStatement;
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error>;
    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + Send + Sync + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Send + Sync,
        I::IntoIter: ExactSizeIterator;
    async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement;
}

impl<W: PreparedClient> PrepareCallback<Statement, Statement, PgTypeMetadata> for &W
where
    W: Send + Sync,
{
    async fn prepare(&mut self, sql: &str, metadata: &[PgTypeMetadata]) -> QueryResult<Statement> {
        let bind_types = metadata
            .iter()
            .map(type_from_oid)
            .collect::<QueryResult<Vec<_>>>()?;

        let stmt = self
            .prepare_typed(sql, &bind_types)
            .await
            .map_err(ErrorHelper)?;
        Ok(stmt)
    }
    async fn raw(&mut self, sql: &str, metadata: &[PgTypeMetadata]) -> QueryResult<Statement> {
        self.prepare(sql, metadata).await
    }
}

fn type_from_oid(t: &PgTypeMetadata) -> QueryResult<Type> {
    let oid = t
        .oid()
        .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

    if let Some(tpe) = Type::from_oid(oid) {
        return Ok(tpe);
    }

    Ok(Type::new(
        "diesel_custom_type".into(),
        oid,
        tokio_postgres::types::Kind::Simple,
        "public".into(),
    ))
}
