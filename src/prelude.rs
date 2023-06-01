pub use google_bigquery_v2_derive::BigDataTableDerive;

pub use crate::client::BigqueryClient;
pub use crate::data::{BigQueryTable, BigQueryTableBase, OrderDirection};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(not(feature = "tracing"))]
pub use log::{trace, info, warn, error, debug};
#[cfg(feature = "tracing")]
pub use tracing::{trace, info, warn, error, debug};

pub use log::LevelFilter;