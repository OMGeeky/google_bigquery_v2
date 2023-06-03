pub use google_bigquery_v2_derive::BigDataTableDerive;

pub use crate::client::BigqueryClient;
pub use crate::data::{BigQueryTable, BigQueryTableBase, OrderDirection};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(not(feature = "tracing"))]
pub use log::{debug, error, info, trace, warn};
#[cfg(feature = "tracing")]
pub use tracing::{debug, error, info, trace, warn};

pub use log::LevelFilter;
