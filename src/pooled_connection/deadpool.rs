//! A connection pool implementation for `benzin` based on [`deadpool`]
//!
//! ```rust
//! # include!("../doctest_setup.rs");
//! use diesel::result::Error;
//! use futures_util::FutureExt;
//! use benzin::pooled_connection::AsyncDieselConnectionManager;
//! use benzin::pooled_connection::deadpool::Pool;
//! use benzin::{RunQueryDsl, AsyncConnection};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! #     run_test().await.unwrap();
//! # }
//! #
//! # fn get_config() -> AsyncDieselConnectionManager<TestConnection> {
//! #    let config = AsyncDieselConnectionManager::<TestConnection>::new("test");
//! #     config
//! #  }
//! #
//! # async fn run_test() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//! #     use schema::users::dsl::*;
//! #     let config = get_config();
//! let pool = Pool::builder(config).build()?;
//! let mut conn = pool.get().await?;
//! # conn.expected_load(vec![vec![String::from("1"), String::from("test")]]);
//! let res = users.load::<(i32, String)>(&mut *conn).await?;
//! #     Ok(())
//! # }
//! ```
use deadpool::managed::Manager;

use super::AsyncDieselConnectionManager;
use crate::AsyncConnection;

/// Type alias for using [`deadpool::managed::Pool`] with `benzin`
pub type Pool<C> = deadpool::managed::Pool<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::PoolBuilder`] with `benzin`
pub type PoolBuilder<C> = deadpool::managed::PoolBuilder<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::BuildError`] with `benzin`
pub type BuildError = deadpool::managed::BuildError;
/// Type alias for using [`deadpool::managed::PoolError`] with `benzin`
pub type PoolError = deadpool::managed::PoolError<super::PoolError>;
/// Type alias for using [`deadpool::managed::Object`] with `benzin`
pub type Object<C> = deadpool::managed::Object<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::Hook`] with `benzin`
pub type Hook<C> = deadpool::managed::Hook<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::HookError`] with `benzin`
pub type HookError = deadpool::managed::HookError<super::PoolError>;

impl<C> Manager for AsyncDieselConnectionManager<C>
where
    C: AsyncConnection + 'static,
{
    type Type = C;

    type Error = super::PoolError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        (self.manager_config.custom_setup)(&self.connection_url)
            .await
            .map_err(super::PoolError::ConnectionError)
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}
