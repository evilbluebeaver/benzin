//! A pool implementation for `benzin` based on [`bb8`]
//!
//! ```rust
//! # include!("../doctest_setup.rs");
//! use diesel::result::Error;
//! use futures_util::FutureExt;
//! use benzin::pooled_connection::AsyncDieselConnectionManager;
//! use benzin::pooled_connection::bb8::Pool;
//! use benzin::{RunQueryDsl, AsyncConnection};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! #     run_test().await.unwrap();
//! # }
//! #
//! # fn get_config() -> AsyncDieselConnectionManager<TestConnection> {
//! let config = AsyncDieselConnectionManager::<TestConnection>::new("test");
//! #     config
//! #  }
//! #
//! # async fn run_test() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//! #     use schema::users::dsl::*;
//! #     let config = get_config();
//! let pool = Pool::builder().build(config).await?;
//! let mut conn = pool.get().await?;
//! # conn.expected_load(vec![vec![String::from("1"), String::from("test")]]);
//! let res = users.load::<(i32, String)>(&mut *conn).await?;
//! #     Ok(())
//! # }
//! ```

use bb8::ManageConnection;

use super::{AsyncDieselConnectionManager, PoolError};
use crate::AsyncConnection;

/// Type alias for using [`bb8::Pool`] with `benzin`
pub type Pool<C> = bb8::Pool<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`bb8::PooledConnection`] with `benzin`
pub type PooledConnection<'a, C> = bb8::PooledConnection<'a, AsyncDieselConnectionManager<C>>;
/// Type alias for using [`bb8::RunError`] with `benzin`
pub type RunError = bb8::RunError<super::PoolError>;

impl<C> ManageConnection for AsyncDieselConnectionManager<C>
where
    C: AsyncConnection + 'static,
{
    type Connection = C;

    type Error = PoolError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        (self.manager_config.custom_setup)(&self.connection_url)
            .await
            .map_err(PoolError::ConnectionError)
    }
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if conn.is_broken() {
            return Err(super::PoolError::DisconnectionError);
        }
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_broken()
    }
}
