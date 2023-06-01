use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use google_bigquery2::api::{ErrorProto, QueryParameter, QueryRequest};
use google_bigquery2::hyper::{Body, Response};
use crate::prelude::*;
use serde_json::Value;

use crate::data::param_conversion::BigDataValueType;
use crate::prelude::*;

//region BigqueryError
#[derive(Debug, Clone)]
pub struct BigqueryError {
    pub message: String,
    pub errors: Option<Vec<ErrorProto>>,
}

impl BigqueryError {
    fn new(message: &str, errors: Option<Vec<ErrorProto>>) -> Self {
        Self {
            message: message.to_string(),
            errors,
        }
    }
}

impl Display for BigqueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BigqueryError: {}", self.message)
    }
}

impl Error for BigqueryError {}

//endregion

//region typestate
//region QueryResultType
#[derive(Debug)]
pub enum QueryResultType<Table> {
    WithRowData(Vec<Table>),
    WithoutRowData(Result<()>),
}

impl<T> QueryResultType<T> {
    pub fn map_err_with_data(self, message: impl Into<String>) -> Result<Vec<T>> {
        match self {
            QueryResultType::WithRowData(data) => Ok(data),
            QueryResultType::WithoutRowData(_) => {
                Err(format!("map_err_with_data message:{}", message.into()).into())
            }
        }
    }
    pub fn map_err_without_data(self, message: impl Into<String>) -> Result<()> {
        match self {
            QueryResultType::WithoutRowData(result) => result,
            QueryResultType::WithRowData(_) => {
                Err(format!("map_err_without_data message:{}", message.into()).into())
            }
        }
    }
    pub fn expect_with_data(self, message: impl Into<String>) -> Vec<T> {
        match self {
            QueryResultType::WithRowData(data) => data,
            QueryResultType::WithoutRowData(_) => {
                panic!("expect_with_data message:{}", message.into())
            }
        }
    }
    pub fn expect_without_data(self, message: impl Into<String>) -> Result<()> {
        match self {
            QueryResultType::WithoutRowData(result) => result,
            QueryResultType::WithRowData(_) => {
                panic!("expect_without_data message:{}", message.into())
            }
        }
    }
    pub fn is_with_row_data(&self) -> bool {
        match self {
            QueryResultType::WithRowData(_) => true,
            QueryResultType::WithoutRowData(_) => false,
        }
    }
    pub fn is_without_row_data(&self) -> bool {
        match self {
            QueryResultType::WithRowData(_) => false,
            QueryResultType::WithoutRowData(_) => true,
        }
    }
}
//endregion
//region typestate structs

#[derive(Debug, Default, Clone)]
pub struct HasStartingData<Table: Default>(Table);

#[derive(Debug, Default, Clone)]
pub struct NoStartingData;

#[derive(Debug, Default, Clone)]
pub struct HasClient(BigqueryClient);

#[derive(Debug, Default, Clone)]
pub struct NoClient;

#[derive(Debug, Default, Clone)]
pub struct QueryWasBuilt;

#[derive(Debug, Default, Clone)]
pub struct QueryWasNotBuilt;

#[derive(Debug, Default, Clone)]
pub struct QueryTypeNoType;

impl HasNoQueryType for QueryTypeNoType {}

//region insert

#[derive(Debug, Clone)]
pub struct QueryTypeInsert;

impl HasQueryType for QueryTypeInsert {}

//endregion
//region select

#[derive(Debug, Clone)]
pub struct QueryTypeSelect;

impl HasQueryType for QueryTypeSelect {}

//endregion
//region update
#[derive(Debug, Clone)]
pub struct QueryTypeUpdate;

impl HasQueryType for QueryTypeUpdate {}

//endregion
//region update
#[derive(Debug, Clone)]
pub struct QueryTypeDelete;

impl HasQueryType for QueryTypeDelete {}

//endregion

//endregion

pub trait HasQueryType {}

pub trait HasNoQueryType {}
//endregion

//region QueryBuilder
#[derive(Debug, Clone)]
pub struct QueryBuilder<Table, QueryType, Client, QueryBuilt, StartingData> {
    client: Client,
    query: String,
    params: Vec<QueryParameter>,
    where_clauses: Vec<String>,
    order_by: Vec<(String, OrderDirection)>,
    limit: Option<u32>,

    starting_data: StartingData,

    query_type: PhantomData<QueryType>,
    query_built: PhantomData<QueryBuilt>,
    table: PhantomData<Table>,
}

//region default implementation for QueryBuilder
impl<Table, QueryType, Client: Default, QueryBuilt, StartingData: Default> Default
    for QueryBuilder<Table, QueryType, Client, QueryBuilt, StartingData>
{
    fn default() -> Self {
        Self {
            client: Client::default(),
            query: String::new(),
            params: Vec::new(),
            where_clauses: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            starting_data: Default::default(),
            query_type: PhantomData,
            query_built: PhantomData,
            table: PhantomData,
        }
    }
}

//endregion
//region general QueryBuilder
//region functions for all queries
impl<Table: BigQueryTable, UnknownQueryType, Client, QueryBuilt, StartingData>
    QueryBuilder<Table, UnknownQueryType, Client, QueryBuilt, StartingData>
{
    fn get_sorted_selected_fields(&self) -> Vec<(String, String)> {
        trace!("get_sorted_selected_fields()");
        let mut fields: Vec<(String, String)> = Table::get_query_fields(true).into_iter().collect();
        debug!("fields: {:?}", fields);
        fields.sort();
        fields
    }

    fn get_fields_string(&self) -> String {
        trace!("get_fields_string()");
        let mut fields = self.get_sorted_selected_fields();
        fields
            .into_iter()
            .map(|f| f.1)
            .collect::<Vec<String>>()
            .join(", ")
    }
}

//endregion
//region functions for not built queries
//region with Starting data
impl<Table: BigQueryTable + Default, UnknownQueryType, Client>
    QueryBuilder<Table, UnknownQueryType, Client, QueryWasNotBuilt, HasStartingData<Table>>
{
    pub fn add_field_where(self, field: &str) -> Result<Self> {
        trace!("add_field_where(field: {})", field);

        let field_db_name = Table::get_field_db_name(field)?;
        let param = Table::get_parameter_from_field(&self.starting_data.0, &field)?;
        let mut params = self.params;

        let mut wheres = self.where_clauses;
        let mut has_param_value = false;
        if let Some(param) = param {
            if param.parameter_value.is_some() {
                has_param_value = true;
                let param_name = param.name.as_ref().unwrap().to_string();
                params.push(param);
                wheres.push(format!("{} = @{}", field_db_name, param_name));
            }
        }
        if !has_param_value {
            wheres.push(format!("{} is NULL", field_db_name));
        }
        Ok(Self {
            where_clauses: wheres,
            params,
            ..self
        })
    }

    fn add_params_for_table_query_fields(&mut self) -> Result<()> {
        trace!("add_params_for_table_query_fields()");
        let local_fields = Table::get_query_fields(true);
        let starting_data = &self.starting_data.0;
        for (local_field_name, _) in local_fields {
            let para = Table::get_parameter_from_field(starting_data, &local_field_name)?;
            if let Some(para) = para {
                let mut has_param = false;
                for existing_para in &self.params {
                    if existing_para.name == para.name {
                        has_param = true;
                        break;
                    }
                }
                if !has_param {
                    self.params.push(para);
                }
            }
        }
        Ok(())
    }
}

//endregion
impl<Table: BigQueryTable + Debug, UnknownQueryType: Debug, Client: Debug, StartingData: Debug>
    QueryBuilder<Table, UnknownQueryType, Client, QueryWasNotBuilt, StartingData>
{
    //region set query content
    pub fn add_where_eq<T>(self, column: &str, value: Option<&T>) -> Result<Self>
    where
        T: BigDataValueType + Debug,
    {
        trace!("add_where_eq({:?}, {:?})", column, value);
        let column = Table::get_field_db_name(column)?;
        let mut wheres = self.where_clauses;

        if let Some(value) = value {
            let param_name = format!("__PARAM_{}", self.params.len());
            let param = Table::get_parameter(value, &param_name);
            if let Some(param) = param {
                let mut required_params = self.params;
                required_params.push(param);

                wheres.push(format!("{} = @{}", column, param_name));

                return Ok(Self {
                    where_clauses: wheres,
                    params: required_params,
                    ..self
                });
            }
        }

        wheres.push(format!("{} is NULL", column));
        Ok(Self {
            where_clauses: wheres,
            ..self
        })
    }

    pub fn set_limit(self, limit: u32) -> Self {
        trace!("set_limit({:?})", limit);
        Self {
            limit: Some(limit),
            ..self
        }
    }
    //endregion

    //region build query
    fn build_where_string(&self) -> String {
        trace!("build_where_string: {:?}", self);
        let mut where_string = String::new();
        if !self.where_clauses.is_empty() {
            where_string.push_str(" WHERE ");
            where_string.push_str(&self.where_clauses.join(" AND "));
        }
        where_string
    }
    fn build_order_by_string(&self) -> Result<String> {
        trace!("build_order_by_string: {:?}", self);
        let mut order_by_string = String::new();
        if !self.order_by.is_empty() {
            order_by_string.push_str(" ORDER BY ");
            let mut order_by = vec![];
            for (column, direction) in &self.order_by {
                let column = Table::get_field_db_name(&column)?;
                order_by.push(format!("{} {}", column, direction.to_query_str()));
            }

            order_by_string.push_str(&order_by.join(", "));
        }
        Ok(order_by_string)
    }
    fn build_limit_string(&self) -> String {
        trace!("build_limit_string: {:?}", self);
        let mut limit_string = String::new();
        if let Some(limit) = self.limit {
            limit_string.push_str(" LIMIT ");
            limit_string.push_str(&limit.to_string());
        }
        limit_string
    }
    //endregion
}

//endregion
//endregion
//region set_data
impl<Table: BigQueryTable + Default + Debug, QueryType: HasQueryType, Client: Default>
    QueryBuilder<Table, QueryType, Client, QueryWasNotBuilt, NoStartingData>
{
    pub fn set_data(
        self,
        data: Table,
    ) -> QueryBuilder<Table, QueryType, Client, QueryWasNotBuilt, HasStartingData<Table>> {
        trace!("set_data({:?})", data);
        QueryBuilder {
            starting_data: HasStartingData(data),
            query_built: PhantomData,
            params: self.params,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            query_type: PhantomData,
            table: PhantomData,
            client: self.client,
            query: self.query,
        }
    }
}

//endregion
//region QueryTypeNoType
impl<Table: BigQueryTable, Client: Default, StartingData: Default>
    QueryBuilder<Table, QueryTypeNoType, Client, QueryWasNotBuilt, StartingData>
{
    pub fn select() -> QueryBuilder<Table, QueryTypeSelect, NoClient, QueryWasNotBuilt, StartingData>
    {
        trace!("select()");
        QueryBuilder {
            ..Default::default()
        }
    }
    pub fn insert() -> QueryBuilder<Table, QueryTypeInsert, NoClient, QueryWasNotBuilt, StartingData>
    {
        trace!("insert()");
        QueryBuilder {
            ..Default::default()
        }
    }
    pub fn update() -> QueryBuilder<Table, QueryTypeUpdate, NoClient, QueryWasNotBuilt, StartingData>
    {
        trace!("update()");
        QueryBuilder {
            ..Default::default()
        }
    }
    pub fn delete() -> QueryBuilder<Table, QueryTypeDelete, NoClient, QueryWasNotBuilt, StartingData>
    {
        trace!("delete()");
        QueryBuilder {
            ..Default::default()
        }
    }
}

//endregion
//region QueryTypeInsert
impl<Table: BigQueryTable + Default + Debug>
    QueryBuilder<Table, QueryTypeDelete, HasClient, QueryWasNotBuilt, HasStartingData<Table>>
{
    pub fn build_query(
        mut self,
    ) -> Result<
        QueryBuilder<Table, QueryTypeDelete, HasClient, QueryWasBuilt, HasStartingData<Table>>,
    > {
        trace!("build_query: delete: {:?}", self);
        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        self = self.add_field_where(&Table::get_pk_field_name())?;
        let where_clause = &self.build_where_string();

        let query = format!("DELETE FROM {} {}", table_identifier, where_clause);
        Ok(QueryBuilder {
            query,
            params: self.params,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            client: self.client,
            table: self.table,
            starting_data: self.starting_data,
            query_type: self.query_type,
            query_built: PhantomData,
        })
    }
}

//region QueryTypeInsert
impl<Table: BigQueryTable + Default + Debug>
    QueryBuilder<Table, QueryTypeInsert, HasClient, QueryWasNotBuilt, HasStartingData<Table>>
{
    pub fn build_query(
        mut self,
    ) -> Result<
        QueryBuilder<Table, QueryTypeInsert, HasClient, QueryWasBuilt, HasStartingData<Table>>,
    > {
        trace!("build_query: insert: {:?}", self);
        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        let params = &self.params;
        warn!("params are not used in insert query: {:?}", params);
        self.add_params_for_table_query_fields()?;
        let fields = self.get_fields_string();
        let values = self.get_values_params_string()?;

        let query = format!(
            "insert into {} ({}) values({})",
            table_identifier, fields, values
        );
        Ok(QueryBuilder {
            query,
            params: self.params,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            client: self.client,
            table: self.table,
            starting_data: self.starting_data,
            query_type: self.query_type,
            query_built: PhantomData,
        })
    }

    fn get_values_params_string(&self) -> Result<String> {
        trace!("get_values_params_string\tself: {:?}", self);
        let values: Vec<Option<String>> = self.get_value_parameter_names()?;
        Ok(values
            .iter()
            .map(|v| match v {
                Some(v) => format!("@{}", v),
                None => String::from("NULL"),
            })
            .collect::<Vec<String>>()
            .join(", "))
    }
    /// Returns a vector of parameter names for the values in the insert query.
    ///
    /// If the parameter for a field does not exists, it will just be NULL in
    /// the query, not a parameter.
    fn get_value_parameter_names(&self) -> Result<Vec<Option<String>>> {
        trace!("get_value_parameter_names\tself: {:?}", self);
        let mut values = self.get_sorted_selected_fields();
        let existing_params: Vec<String> = self
            .params
            .iter()
            .map(|p| p.name.clone().unwrap())
            .collect();
        debug!(
            "existing_params: len: {} params: {:?}",
            existing_params.len(),
            existing_params
        );
        debug!(
            "selected_fields: len: {} fields: {:?}",
            values.len(),
            values
        );
        let res = values
            .iter_mut()
            .map(|(field, _)| match Table::get_field_param_name(field) {
                Ok(param_name) => {
                    if existing_params.contains(&param_name) {
                        Ok(Some(param_name))
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<Option<String>>>>()?;
        Ok(res)
    }
}

//endregion
//region QueryTypeUpdate
impl<Table: BigQueryTable + Default + Debug>
    QueryBuilder<Table, QueryTypeUpdate, HasClient, QueryWasNotBuilt, HasStartingData<Table>>
{
    pub fn build_query(
        mut self,
    ) -> Result<
        QueryBuilder<Table, QueryTypeUpdate, HasClient, QueryWasBuilt, HasStartingData<Table>>,
    > {
        trace!("build_query: update: {:?}", self);
        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        if self.where_clauses.is_empty() {
            trace!("no where clause, adding pk field to where clause");
            self = self.add_field_where(&Table::get_pk_field_name())?;
        }
        let where_clause = self.build_where_string();
        let params = &self.params;
        warn!("params are not used in update query: {:?}", params);
        self.add_params_for_table_query_fields()?;
        let fields_str = self.build_update_fields_string()?;

        let query = format!(
            "update {} set {} {}",
            table_identifier, fields_str, where_clause
        );
        Ok(QueryBuilder {
            query,
            params: self.params,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            client: self.client,
            table: self.table,
            starting_data: self.starting_data,
            query_type: self.query_type,
            query_built: PhantomData,
        })
    }

    fn build_update_fields_string(&mut self) -> Result<String> {
        trace!("build_update_fields_string");
        let result = self
            .get_value_parameter_names()?
            .into_iter()
            .map(|(f, p)| match p {
                Some(p) => format!("{} = @{}", f, p),
                None => format!("{} = NULL", f),
            })
            .collect::<Vec<String>>()
            .join(", ");
        trace!("build_update_fields_string: result: {}", result);
        Ok(result)
    }

    fn get_value_parameter_names(&self) -> Result<Vec<(String, Option<String>)>> {
        let mut values = self.get_sorted_selected_fields();
        let existing_params: Vec<String> = self
            .params
            .iter()
            .map(|p| p.name.clone().unwrap())
            .collect();
        let mut res = vec![];
        for (field, _) in values.iter_mut() {
            res.push((
                Table::get_field_db_name(field)?,
                match existing_params.contains(&Table::get_field_param_name(field)?) {
                    true => Some(Table::get_field_param_name(field)?),
                    false => None,
                },
            ));
        }
        Ok(res)
    }
}

//endregion
//region QueryTypeSelect
//region client not needed
impl<Table: BigQueryTable + Debug, Client: Debug, StartingData: Debug>
    QueryBuilder<Table, QueryTypeSelect, Client, QueryWasNotBuilt, StartingData>
{
    pub fn add_order_by(
        mut self,
        column_name: impl Into<String>,
        direction: OrderDirection,
    ) -> Self {
        self.order_by.push((column_name.into(), direction));
        self
    }
}

//endregion
//region client needed
impl<Table: BigQueryTable + Debug, StartingData: Debug>
    QueryBuilder<Table, QueryTypeSelect, HasClient, QueryWasNotBuilt, StartingData>
{
    pub fn build_query(
        self,
    ) -> Result<QueryBuilder<Table, QueryTypeSelect, HasClient, QueryWasBuilt, StartingData>> {
        trace!("build_query: select: {:?}", self);

        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        let fields_str = self.get_fields_string();
        let where_clause = self.build_where_string();
        let order_by_clause = self.build_order_by_string()?;
        let limit_clause = self.build_limit_string();
        let query = format!(
            "SELECT {} FROM {}{}{}{}",
            fields_str, table_identifier, where_clause, order_by_clause, limit_clause
        );
        Ok(QueryBuilder {
            query,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            client: self.client,
            params: self.params,
            table: self.table,
            starting_data: self.starting_data,
            query_type: self.query_type,
            query_built: PhantomData,
        })
    }
}

//endregion
//endregion
//region with_client
impl<Table: BigQueryTable, QueryType, StartingData>
    QueryBuilder<Table, QueryType, NoClient, QueryWasNotBuilt, StartingData>
{
    pub fn with_client(
        self,
        client: BigqueryClient,
    ) -> QueryBuilder<Table, QueryType, HasClient, QueryWasNotBuilt, StartingData> {
        QueryBuilder {
            client: HasClient(client),
            table: self.table,
            query_type: self.query_type,
            query_built: self.query_built,
            query: self.query,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            params: self.params,
            starting_data: self.starting_data,
        }
    }
}

//endregion
//region un_build & get query string
impl<Table: BigQueryTable, QueryType, Client, StartingData>
    QueryBuilder<Table, QueryType, Client, QueryWasBuilt, StartingData>
{
    pub fn un_build(
        self,
    ) -> QueryBuilder<Table, QueryType, Client, QueryWasNotBuilt, StartingData> {
        QueryBuilder {
            client: self.client,
            table: self.table,
            query_type: self.query_type,
            query: self.query,
            where_clauses: self.where_clauses,
            order_by: self.order_by,
            limit: self.limit,
            params: self.params,
            starting_data: self.starting_data,
            query_built: PhantomData,
        }
    }
    pub fn get_query_string(&self) -> &str {
        &self.query
    }
}

//endregion
//region run
impl<Table: BigQueryTable, QueryType: HasQueryType, StartingData>
    QueryBuilder<Table, QueryType, HasClient, QueryWasBuilt, StartingData>
{
    pub async fn run(self) -> Result<QueryResultType<Table>> {
        trace!("run query: {}", self.query);
        debug!(
            "Running query with params: {}\t params: {:?}",
            self.query, self.params
        );
        let sorted_fields = self.get_sorted_selected_fields();
        let query = Some(self.query);
        let query_parameters = match self.params.is_empty() {
            true => None,
            false => Some(self.params),
        };
        let query_request = QueryRequest {
            query,
            query_parameters,
            use_legacy_sql: Some(false),
            ..Default::default()
        };
        let client = self.client.0;
        debug!("query_request: {:?}", query_request);
        let (_, query_response) = run_query_with_client(&client, query_request).await?;
        // if let Some(errors) = query_response.errors {
        //     return Err(BigqueryError::new("Query returned errors", Some(errors)).into());
        // }
        debug!(
            "total rows returned: {}",
            query_response.total_rows.unwrap_or(0)
        );
        //TODO: pagination is not implemented
        let mut result: Vec<Table> = vec![];
        for row in query_response.rows.unwrap_or_default() {
            let mut row_result: HashMap<String, Value> = HashMap::new();
            for (i, field) in row.f.unwrap_or_default().into_iter().enumerate() {
                let field_db_name = sorted_fields[i].1.clone();
                let field_value = field.v.unwrap_or(Value::Null);
                row_result.insert(field_db_name, field_value);
            }
            let row_result = Table::new_from_query_result_row(client.clone(), &row_result)?;
            result.push(row_result);
        }
        debug!("total rows parsed: {}", result.len());

        Ok(QueryResultType::WithRowData(result))
    }
}
//endregion
//endregion

//region extra helper functions
async fn run_query_with_client(
    client: &BigqueryClient,
    request: QueryRequest,
) -> Result<(Response<Body>, google_bigquery2::api::QueryResponse)> {
    let project_id = client.get_project_id();
    let (response, query_response) = client
        .get_client()
        .jobs()
        .query(request, project_id)
        .doit()
        .await?;

    if response.status() != 200 {
        return Err(format!("Wrong status code returned! ({})", response.status()).into());
    }

    Ok((response, query_response))
}

//endregion
