pub use google_bigquery_v2_derive::BigDataTableDerive;

pub use crate::client::BigqueryClient;
pub use crate::data::{bigquery_builder::{
    BigQueryBuilder,
    BigQueryBuilderAvailable,
    OrderDirection,
},
                      BigQueryTable,
                      BigQueryTableBase};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
