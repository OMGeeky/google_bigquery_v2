use std::collections::HashMap;

use log::trace;
use serde_json::Value;

use crate::client::BigqueryClient;
use crate::data::param_conversion::{BigDataValueType, ConversionError};
use crate::data::param_conversion::ConvertBigQueryParams;
use crate::prelude::*;


pub trait BigQueryTableBase<'a> {
    fn get_table_name() -> String;
    fn get_client(&self) -> &'a BigqueryClient;
    fn set_client(&mut self, client: &'a BigqueryClient);
    fn get_pk_field_name() -> String;
    fn get_pk_db_name() -> String;
    fn get_pk_value(&self) -> &dyn BigDataValueType;
    fn get_query_fields(include_pk: bool) -> HashMap<String, String>;
    fn set_field_value(&mut self, field_name: &str, value: &Value) -> Result<()>;

    fn new_from_query_result_row(
        client: &'a BigqueryClient,
        row: &HashMap<String, Value>,
    ) -> Result<Self>
        where
            Self: Sized;
}

pub trait BigQueryTable<'a>: BigQueryTableBase<'a> {
    fn get_field_db_name(field_name: &str) -> Result<String> {
        trace!("get_field_db_name({})", field_name);
        let query_fields = Self::get_query_fields(true);
        let db_name = query_fields.get(field_name);
        match db_name {
            None => Err(format!("Field {} not found.", field_name).into()),
            Some(s) => Ok(s.to_string()),
        }
    }

    fn get_table_identifier(&self) -> String {
        trace!("get_table_identifier()");
        Self::get_table_identifier_from_client(self.get_client())
    }

    fn get_table_identifier_from_client(client: &'a BigqueryClient) -> String {
        trace!("get_table_identifier_from_client({:?})", client);
        format!(
            "`{}.{}.{}`",
            client.get_project_id(),
            client.get_dataset_id(),
            Self::get_table_name()
        )
    }
}

impl<'a, T> BigQueryTable<'a> for T where T: BigQueryTableBase<'a> {}
