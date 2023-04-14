use log::warn;
use std::fmt::{Debug, Display};

use serde_json::Value;

use crate::data::param_conversion::ConvertBigQueryParams;

pub trait ConvertTypeToBigQueryType {
    fn convert_type_to_bigquery_type() -> String
    where
        Self: Sized;
}

impl ConvertTypeToBigQueryType for bool {
    fn convert_type_to_bigquery_type() -> String {
        "BOOL".to_string()
    }
}

impl ConvertTypeToBigQueryType for i32 {
    fn convert_type_to_bigquery_type() -> String {
        "INT64".to_string()
    }
}

impl ConvertTypeToBigQueryType for i64 {
    fn convert_type_to_bigquery_type() -> String {
        "INT64".to_string()
    }
}

impl ConvertTypeToBigQueryType for u64 {
    fn convert_type_to_bigquery_type() -> String {
        "INT64".to_string()
    }
}

impl ConvertTypeToBigQueryType for f64 {
    fn convert_type_to_bigquery_type() -> String {
        "DOUBLE".to_string() //TODO: check if this is correct
    }
}

impl ConvertTypeToBigQueryType for String {
    fn convert_type_to_bigquery_type() -> String {
        "STRING".to_string()
    }
}

impl ConvertTypeToBigQueryType for &str {
    fn convert_type_to_bigquery_type() -> String {
        "STRING".to_string()
    }
}

impl<T> ConvertTypeToBigQueryType for chrono::DateTime<T>
where
    T: chrono::TimeZone + Display + Send + Sync + 'static,
{
    fn convert_type_to_bigquery_type() -> String {
        "DATETIME".to_string()
    }
}
