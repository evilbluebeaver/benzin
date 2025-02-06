//! This module contains support using benzin with
//! various async rust connection pooling solutions
//!
//! See the concrete pool implementations for examples:
//! * [deadpool]
//! * [bb8]
use std::fmt;

use futures_util::{future::BoxFuture, FutureExt};

use crate::AsyncConnection;

#[cfg(feature = "bb8")]
pub mod bb8;
#[cfg(feature = "deadpool")]
pub mod deadpool;

/// The error used when managing connections with `deadpool`.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum PoolError {
    /// An error occurred establishing the connection
    ConnectionError(#[from] diesel::result::ConnectionError),

    /// An error occurred pinging the database
    QueryError(#[from] diesel::result::Error),
}

/// Type of the custom setup closure passed to [`ManagerConfig::custom_setup`]
pub type SetupCallback<C> =
    Box<dyn Fn(&str) -> BoxFuture<diesel::ConnectionResult<C>> + Send + Sync>;

/// Configuration object for a Manager.
///
#[non_exhaustive]
pub struct ManagerConfig<C> {
    /// Construct a new connection manger
    /// with a custom setup procedure
    ///
    /// This can be used to for example establish a SSL secured
    /// postgres connection
    pub custom_setup: SetupCallback<C>,
}

impl<C> Default for ManagerConfig<C>
where
    C: AsyncConnection + 'static,
{
    fn default() -> Self {
        Self {
            custom_setup: Box::new(|url| C::establish(url).boxed()),
        }
    }
}

/// An connection manager for use with benzin.
///
/// See the concrete pool implementations for examples:
/// * [deadpool]
/// * [bb8]
pub struct AsyncDieselConnectionManager<C> {
    connection_url: String,
    manager_config: ManagerConfig<C>,
}

impl<C> fmt::Debug for AsyncDieselConnectionManager<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AsyncDieselConnectionManager<{}>",
            std::any::type_name::<C>()
        )
    }
}

impl<C> AsyncDieselConnectionManager<C>
where
    C: AsyncConnection + 'static,
{
    /// Returns a new connection manager,
    /// which establishes connections to the given database URL.
    #[must_use]
    pub fn new(connection_url: impl Into<String>) -> Self
    where
        C: AsyncConnection + 'static,
    {
        Self::new_with_config(connection_url, Default::default())
    }

    /// Returns a new connection manager,
    /// which establishes connections with the given database URL
    /// and that uses the specified configuration
    #[must_use]
    pub fn new_with_config(
        connection_url: impl Into<String>,
        manager_config: ManagerConfig<C>,
    ) -> Self {
        Self {
            connection_url: connection_url.into(),
            manager_config,
        }
    }
}
