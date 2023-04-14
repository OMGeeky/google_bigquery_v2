use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub use convert_bigquery_params::{
    convert_value_to_string, ConvertBigQueryParams,
};
pub use convert_type_to_big_query_type::ConvertTypeToBigQueryType;

mod convert_bigquery_params;
mod convert_type_to_big_query_type;

pub trait BigDataValueType:
ConvertTypeToBigQueryType + ConvertBigQueryParams + Debug + Send + Sync
{}

impl<T: ConvertTypeToBigQueryType + ConvertBigQueryParams + Debug + Send + Sync> BigDataValueType
for T
{}

//region ConversionError
#[derive(Debug)]
pub struct ConversionError {
    pub message: String,
}

impl Display for ConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.message))
    }
}

impl Error for ConversionError {}

impl From<&str> for ConversionError {
    fn from(message: &str) -> Self {
        ConversionError::new(message)
    }
}

impl ConversionError {
    pub fn new(message: impl Into<String>) -> Self {
        ConversionError {
            message: message.into(),
        }
    }
}

//endregion
