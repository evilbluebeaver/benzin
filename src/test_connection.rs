use std::io::Write;

use diesel::{
    backend::{sql_dialect, Backend, DieselReserveSpecialization, SqlDialect, TrustedBackend},
    deserialize::FromSql,
    query_builder::{
        bind_collector::RawBytesBindCollector, AstPass, IntoUpdateTarget, LimitOffsetClause,
        QueryBuilder, QueryFragment,
    },
    row::{Field, PartialRow, Row, RowIndex, RowSealed},
    serialize::ToSql,
    sql_types::{
        BigInt, Binary, Date, Double, Float, HasSqlType, Integer, SmallInt, Text, Time, Timestamp,
        TypeMetadata,
    },
    AsChangeset, QueryResult,
};
use futures_util::{
    stream::{self, BoxStream},
    StreamExt,
};

use super::{AsyncConnection, AsyncExecute, RunQueryDsl};
use crate::{methods::LoadQuery, AsyncTransaction, AsyncTransactional, UpdateAndFetchResults};

/// A test connection that can be used to test the diesel API
#[derive(Debug, Default)]
pub struct TestConnection {
    expected_load: Option<Vec<Vec<String>>>,
    expected_return_count: Option<usize>,
}

impl TestConnection {
    /// Expect the connection to load the given data
    ///
    /// Resets after loading
    pub fn expected_load(&mut self, value: Vec<Vec<String>>) {
        self.expected_load = Some(value);
    }

    /// Expect the connection to return the given count
    ///
    /// Resets after returning
    pub fn expected_return_count(&mut self, value: usize) {
        self.expected_return_count = Some(value);
    }
}

pub struct TestBackend;

pub struct TestRow {
    data: Vec<String>,
}

pub struct TestField<'f>(&'f str);

impl TypeMetadata for TestBackend {
    type TypeMetadata = ();
    type MetadataLookup = ();
}

impl DieselReserveSpecialization for TestBackend {}

impl HasSqlType<Timestamp> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}

impl HasSqlType<Time> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<Date> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}

impl HasSqlType<Binary> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<Text> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<Double> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<Float> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<BigInt> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<Integer> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}
impl HasSqlType<SmallInt> for TestBackend {
    fn metadata(_lookup: &mut Self::MetadataLookup) -> Self::TypeMetadata {}
}

impl TrustedBackend for TestBackend {}
impl SqlDialect for TestBackend {
    type ReturningClause = sql_dialect::returning_clause::PgLikeReturningClause;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;
    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;

    type InsertWithDefaultKeyword = sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;
    type BatchInsertSupport = sql_dialect::batch_insert_support::PostgresLikeBatchInsertSupport;

    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;

    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;

    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;
}

impl<A, B> QueryFragment<TestBackend> for LimitOffsetClause<A, B> {
    fn walk_ast<'b>(&'b self, _pass: AstPass<'_, 'b, TestBackend>) -> QueryResult<()> {
        Ok(())
    }
}

pub struct TestQueryBuilder;

impl QueryBuilder<TestBackend> for TestQueryBuilder {
    fn push_sql(&mut self, _sql: &str) {}
    fn push_bind_param(&mut self) {}
    fn push_bind_param_value_only(&mut self) {}
    fn push_identifier(&mut self, _identifier: &str) -> QueryResult<()> {
        Ok(())
    }
    fn finish(self) -> String {
        String::new()
    }
}

impl Backend for TestBackend {
    type QueryBuilder = TestQueryBuilder;
    type BindCollector<'a> = RawBytesBindCollector<Self>;
    type RawValue<'a> = String;
}

impl RowSealed for TestRow {}
impl RowIndex<usize> for TestRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        Some(idx)
    }
}
impl RowIndex<&str> for TestRow {
    fn idx(&self, _idx: &str) -> Option<usize> {
        unimplemented!()
    }
}

impl<'f> Field<'f, TestBackend> for TestField<'f> {
    fn field_name(&self) -> Option<&str> {
        unimplemented!()
    }
    fn value(&self) -> Option<<TestBackend as Backend>::RawValue<'_>> {
        Some(self.0.to_string())
    }
}

impl<'a> Row<'a, TestBackend> for TestRow {
    type Field<'f>
        = TestField<'f>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;
    fn field_count(&self) -> usize {
        self.data.len()
    }
    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> diesel::row::PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: diesel::row::RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(TestField(&self.data[idx]))
    }
}

impl AsyncExecute for TestConnection {
    type Stream<'conn> = BoxStream<'conn, QueryResult<TestRow>>;
    type Row<'conn> = TestRow;
    type Backend = TestBackend;
    async fn batch_execute(&mut self, _query: &str) -> QueryResult<()> {
        unimplemented!()
    }
    async fn execute_returning_count<T>(&mut self, _source: T) -> QueryResult<usize>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        self.expected_return_count
            .take()
            .ok_or(diesel::result::Error::NotFound)
    }
    async fn load<T>(&mut self, _source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: diesel::query_builder::AsQuery + Send,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        let data = self
            .expected_load
            .take()
            .ok_or(diesel::result::Error::NotFound)?;
        Ok(stream::iter(data.into_iter().map(|data| Ok(TestRow { data }))).boxed())
    }
}

impl AsyncConnection for TestConnection {
    async fn establish(_database_url: &str) -> diesel::ConnectionResult<Self> {
        Ok(Self::default())
    }
}

impl FromSql<Text, TestBackend> for String {
    fn from_sql(
        bytes: <TestBackend as Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        Ok(bytes)
    }
}

impl FromSql<Integer, TestBackend> for i32 {
    fn from_sql(
        bytes: <TestBackend as Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        Ok(bytes.parse()?)
    }
}

impl ToSql<Integer, TestBackend> for i32 {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, TestBackend>,
    ) -> diesel::serialize::Result {
        out.write_all(&self.to_ne_bytes())?;
        Ok(diesel::serialize::IsNull::No)
    }
}

/// A test transaction that can be used to test the diesel API
#[derive(Debug, Default)]
pub struct TestTransaction {
    expected_load: Option<Vec<Vec<String>>>,
    expected_return_count: Option<usize>,
}

impl TestTransaction {
    /// Expect the connection to load the given data
    ///
    /// Resets after loading
    pub fn expected_load(&mut self, value: Vec<Vec<String>>) {
        self.expected_load = Some(value);
    }

    /// Expect the connection to return the given count
    ///
    /// Resets after returning
    pub fn expected_return_count(&mut self, value: usize) {
        self.expected_return_count = Some(value);
    }
}

impl AsyncTransaction for TestTransaction {
    async fn commit(self) -> QueryResult<()> {
        Ok(())
    }
    async fn rollback(self) -> QueryResult<()> {
        Ok(())
    }
}

impl AsyncExecute for TestTransaction {
    type Stream<'conn> = BoxStream<'conn, QueryResult<TestRow>>;
    type Row<'conn> = TestRow;
    type Backend = TestBackend;

    async fn batch_execute(&mut self, _query: &str) -> QueryResult<()> {
        unimplemented!()
    }
    async fn execute_returning_count<T>(&mut self, _source: T) -> QueryResult<usize>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        self.expected_return_count
            .take()
            .ok_or(diesel::result::Error::NotFound)
    }
    async fn load<T>(&mut self, _source: T) -> QueryResult<Self::Stream<'_>>
    where
        T: diesel::query_builder::AsQuery + Send,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        let data = self
            .expected_load
            .take()
            .ok_or(diesel::result::Error::NotFound)?;
        Ok(stream::iter(data.into_iter().map(|data| Ok(TestRow { data }))).boxed())
    }
}

impl AsyncTransactional for TestConnection {
    type Transaction<'a> = TestTransaction;
    async fn begin_transaction(&mut self) -> QueryResult<Self::Transaction<'_>> {
        Ok(TestTransaction::default())
    }
}

impl AsyncTransactional for TestTransaction {
    type Transaction<'a> = TestTransaction;
    async fn begin_transaction(&mut self) -> QueryResult<Self::Transaction<'_>> {
        Ok(TestTransaction::default())
    }
}

impl<Changes, Output, Tab, V> UpdateAndFetchResults<Changes, Output> for TestConnection
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
