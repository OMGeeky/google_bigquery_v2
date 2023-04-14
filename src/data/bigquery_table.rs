use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use async_trait::async_trait;
pub use google_bigquery2::api::QueryParameter;
use google_bigquery2::api::QueryRequest;
pub use google_bigquery2::api::{QueryParameterType, QueryParameterValue};
use log::debug;
use log::trace;
use serde_json::Value;

use crate::client::BigqueryClient;
use crate::data::param_conversion::{
    convert_value_to_string, BigDataValueType,
};
use crate::prelude::*;

#[async_trait]
pub trait BigQueryTableBase {
    fn get_all_params(&self) -> Result<Vec<QueryParameter>>;
    fn get_parameter_from_field(&self, field_name: &str) -> Result<QueryParameter>;
    //region get infos
    /// Returns the name of the table in the database.
    fn get_table_name() -> String;
    /// Returns the bigquery-client for the struct.
    fn get_client(&self) -> &BigqueryClient;
    /// Sets the bigquery-client for the struct.
    fn set_client(&mut self, client: BigqueryClient);
    /// Returns the name of the primary key field in the struct.
    fn get_pk_field_name() -> String;
    /// Returns the name of the primary key field in the database.
    fn get_pk_db_name() -> String;
    /// Returns the value of the primary key.
    fn get_pk_value(&self) -> &(dyn BigDataValueType + Send + Sync);
    /// Returns a HashMap with the field name as key and the db name as value.
    fn get_query_fields(include_pk: bool) -> HashMap<String, String>;
    async fn reload(&mut self) -> Result<()>;
    //endregion

    //region set infos
    /// Sets the value of a field by its db name.
    fn set_field_value(&mut self, field_name: &str, value: &Value) -> Result<()>;
    fn get_field_value(&self, field_name: &str) -> Result<Value>;
    /// creates a new instance of the struct from a query result row and a bigquery-client.
    ///
    /// # Arguments
    /// * `client` - The bigquery-client to use.
    /// * `row` - The query result row. The keys are the db names of the fields.
    fn new_from_query_result_row(
        client: BigqueryClient,
        row: &HashMap<String, Value>,
    ) -> Result<Self>
    where
        Self: Sized;

    //region update

    //TODO: fn update(&mut self) -> Result<()>;
    //TODO: fn delete(&mut self) -> Result<()>;

    //endregion

    //region insert

    async fn insert(&mut self) -> Result<()>;
    async fn update(&mut self) -> Result<()>;

    //endregion

    //endregion
}

#[async_trait]
pub trait BigQueryTable: BigQueryTableBase {
    fn get_parameter<T>(value: &T, param_name: &String) -> Result<QueryParameter>
    where
        T: BigDataValueType + Debug,
    {
        trace!("get_parameter({:?}, {})", value, param_name);
        let value = value.to_param();
        let param_type = T::convert_type_to_bigquery_type();
        let param_type = QueryParameterType {
            type_: Some(param_type),
            ..Default::default()
        };
        debug!("param_type: {:?}", param_type);
        debug!("param_value: {:?}", value);
        let param_value = convert_value_to_string(value)?;
        debug!("param_value: {:?}", param_value);
        let param_value = QueryParameterValue {
            value: Some(param_value),
            ..Default::default()
        };

        let param = QueryParameter {
            parameter_type: Some(param_type),
            parameter_value: Some(param_value),
            name: Some(param_name.clone()),
        };
        Ok(param)
    }
    fn get_field_param_name(field_name: &str) -> Result<String> {
        trace!("get_field_param_name({})", field_name);
        let db_name = Self::get_field_db_name(field_name)?;
        Ok(format!("__PARAM_{}", db_name))
    }
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

    fn get_table_identifier_from_client(client: &BigqueryClient) -> String {
        trace!("get_table_identifier_from_client({:?})", client);
        format!(
            "`{}.{}.{}`",
            client.get_project_id(),
            client.get_dataset_id(),
            Self::get_table_name()
        )
    }

    async fn get_by_pk<PK>(client: BigqueryClient, pk_value: &PK) -> Result<Self>
    where
        PK: BigDataValueType + Send + Sync + 'static,
        Self: Sized,
    {
        trace!("get_by_pk({:?}, {:?})", client, pk_value);
        let pk_field_name = Self::get_pk_field_name();
        let pk_db_name = Self::get_pk_db_name();
        let result = Self::query(client)
            .add_where_eq(&pk_field_name, Some(pk_value))?
            .run()
            .await;
        match result {
            Ok(mut v) => {
                if v.len() == 0 {
                    Err(format!("No entry found for {} = {:?}", pk_db_name, pk_value).into())
                } else if v.len() > 1 {
                    Err(format!(
                        "More than one entry found for {} = {:?}",
                        pk_db_name, pk_value
                    )
                    .into())
                } else {
                    Ok(v.remove(0))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn upsert(&mut self) -> Result<()>
    where
        Self: Sized + Clone + Send + Sync,
    {
        trace!("upsert()");

        let exists = self.clone().reload().await; //TODO: this is not very efficient
        match exists {
            Ok(_) => {
                debug!("Updating entry on db.");
                self.save().await
            }
            Err(_) => {
                debug!("Inserting new entry.");
                self.insert().await
            }
        }
    }

    /// proxy for update
    async fn save(&mut self) -> Result<()> {
        self.update().await
    }

    /// updates the current instance from another instance.
    /// Does not save the changes to the database.
    fn update_from(&mut self, other: &Self) -> Result<()> {
        for (field_name, _) in Self::get_query_fields(true) {
            let value = other.get_field_value(&field_name)?;
            self.set_field_value(&field_name, &value)?;
        }
        Ok(())
    }

    fn query<Table>(client: BigqueryClient) -> BigQueryBuilder<Table>
    where
        Table: BigQueryTable,
    {
        BigQueryBuilder {
            client: Some(client),
            ..Default::default()
        }
    }
}

impl<T> BigQueryTable for T where T: BigQueryTableBase {}

#[derive(Debug, Clone)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

impl OrderDirection {
    pub(crate) fn to_query_str(&self) -> String {
        match self {
            OrderDirection::Ascending => String::from("ASC"),
            OrderDirection::Descending => String::from("DESC"),
        }
    }
}
