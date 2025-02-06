# An async interface for diesel

Diesel gets rid of the boilerplate for database interaction and eliminates
runtime errors without sacrificing performance. It takes full advantage of
Rust's type system to create a low overhead query builder that "feels like
Rust."

Benzin provides an async implementation of diesels connection implementation
and any method that may issue an query. It is designed as pure async drop-in replacement
for the corresponding diesel methods. Similar to diesel the crate is designed in a way
that allows third party crates to extend the existing infrastructure and even provide
their own connection implementations.

Benzin is a rework of existing library [diesel_async](https://github.com/weiznich/diesel_async/tree/main). Unlike the original it handles transactions properly so there will be no dangling transactions in case of cancelled future. Hence there is no need to check for correctness of a connection taken from pool.

A lot of work has been done to redesign internal structures, traits, tests, etc. Some of the functionality has been dropped in favor of a simpler design. It's doubtful that this can be merged into main project in a reasonable amount of time without breaking changes. That's why it was decided to make separate project.

Supported databases:

1. PostgreSQL
2. MySQL

## Usage

### Simple usage

Benzin is designed to work in combination with diesel, not to replace diesel. For this it
provides drop-in replacement for diesel functionality that actually interacts with the database.

A normal project should use a setup similar to the following one:

```toml
[dependencies]
diesel = "2.2" # no backend features need to be enabled
benzin = { version = "0.1.0", features = ["postgres"] }
```

This allows to import the relevant traits from both crates:

```rust
use diesel::prelude::*;
use benzin::{RunQueryDsl, AsyncConnection, AsyncPgConnection};

// ordinary diesel model setup

table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

// create an async connection
let mut connection = AsyncPgConnection::establish(&std::env::var("DATABASE_URL")?).await?;

// use ordinary diesel query dsl to construct your query
let data: Vec<User> = users::table
    .filter(users::id.gt(0))
    .or_filter(users::name.like("%Luke"))
    .select(User::as_select())
    // execute the query via the provided
    // async `benzin::RunQueryDsl`
    .load(&mut connection)
    .await?;
```

### Async Transaction Support

Benzin provides an ergonomic interface to wrap several statements into a shared
database transaction. Such transactions are automatically rolled back as soon as
the inner closure returns an error and are commited otherwise.

``` rust
use benzin::AsyncTransactional;

connection.transaction(|transaction| Box::pin(async move {
         diesel::insert_into(users::table)
             .values(users::name.eq("Ruby"))
             .execute(transaction)
             .await?;

         let all_names = users::table.select(users::name).load::<String>(transaction).await?;
         Ok::<_, diesel::result::Error>(())
       })
    ).await?;
```

It is posibble to nest a transaction if underlying database supports it. Right now Postgres supports this possibility and MySQL doesn't.

``` rust
use benzin::AsyncTransactional;

connection.transaction(|transaction| Box::pin(async move {
        ...
        transaction.transaction(|inner_transaction| Box::pin(async move {
            diesel::insert_into(users::table)
                .values(users::name.eq("Tess"))
                .execute(inner_transaction)
                .await?;
            Ok::<_, diesel::result::Error>(())
            })
        ).await?
        ...
         Ok::<_, diesel::result::Error>(())
       })
    ).await?;
```

### Streaming Query Support

Beside loading data directly into a vector, benzin also supports returning a
value stream for each query. This allows to process data from the database while they
are still received.

```rust
// use ordinary diesel query dsl to construct your query
let data: impl Stream<Item = QueryResult<User>> = users::table
    .filter(users::id.gt(0))
    .or_filter(users::name.like("%Luke"))
    .select(User::as_select())
    // execute the query via the provided
    // async `benzin::RunQueryDsl`
    .load_stream(&mut connection)
    .await?;

```

### Built-in Connection Pooling Support

Benzin provides built-in support for several connection pooling crates. This includes support
for:

* [deadpool](https://crates.io/crates/deadpool)
* [bb8](https://crates.io/crates/bb8)

#### Deadpool

``` rust
use benzin::pooled_connection::{AsyncDieselConnectionManager, deadpool::Pool};
use benzin::RunQueryDsl;

// create a new connection pool with the default config
let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(std::env::var("DATABASE_URL")?);
let pool = Pool::builder(config).build()?;

// checkout a connection from the pool
let mut conn = pool.get().await?;

// use the connection
// you need to derefmut a reference
let res = users::table.select(User::as_select()).load::(&mut *conn).await?;
```

#### BB8

``` rust
use benzin::pooled_connection::{AsyncDieselConnectionManager, bb8::Pool};
use benzin::RunQueryDsl;

// create a new connection pool with the default config
let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(std::env::var("DATABASE_URL")?);
let pool = Pool::builder().build(config).await?;

// checkout a connection from the pool
let mut conn = pool.get().await?;

// use the connection as ordinary benzin connection
let res = users::table.select(User::as_select()).load::(&mut *conn).await?;
```
## Crate Feature Flags

Benzin offers several configurable features:

* `postgres`: Enables the implementation of `AsyncPgConnection`
* `mysql`: Enables the implementation of `AsyncMysqlConnection`
* `deadpool`: Enables support for the `deadpool` connection pool implementation
* `bb8`: Enables support for the `bb8` connection pool implementation

By default no features are enabled.

## License

Licensed under either of these:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

### Contributing

Contributions are explicitly welcome.