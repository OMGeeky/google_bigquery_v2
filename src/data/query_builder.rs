use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use google_bigquery2::api::{ErrorProto, QueryParameter, QueryRequest};
use google_bigquery2::hyper::{Body, Response};
use log::{debug, trace};
use serde_json::Value;

use crate::data::param_conversion::BigDataValueType;
use crate::prelude::*;

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

#[derive(Debug)]
pub enum QueryResultType<Table> {
    WithRowData(Vec<Table>),
    WithoutRowData(Result<()>),
}

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
// pub struct QueryTypeNoUpdate;
// pub struct QueryTypeUpdate;
// struct QueryTypeNoDelete;
// struct QueryTypeDelete;
//endregion

pub trait HasQueryType {}

pub trait HasNoQueryType {}

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

impl<Table: BigQueryTable, UnknownQueryType, Client, QueryBuilt, StartingData>
QueryBuilder<Table, UnknownQueryType, Client, QueryBuilt, StartingData>
{
    fn get_sorted_selected_fields(&self) -> Vec<(String, String)> {
        trace!("get_sorted_selected_fields()");
        let mut fields: Vec<(String, String)> = Table::get_query_fields(true).into_iter().collect();
        log::debug!("fields: {:?}", fields);
        fields.sort();
        fields
    }

    fn get_fields_string(&self) -> String {
        let mut fields = self.get_sorted_selected_fields();
        fields
            .into_iter()
            .map(|f| f.1)
            .collect::<Vec<String>>()
            .join(", ")
    }
}

impl<Table: BigQueryTable, UnknownQueryType, Client, StartingData>
QueryBuilder<Table, UnknownQueryType, Client, QueryWasNotBuilt, StartingData>
{
    //region set query content
    pub fn add_where_eq<T>(self, column: &str, value: Option<&T>) -> Result<Self>
        where
            T: BigDataValueType + Debug,
    {
        let column = Table::get_field_db_name(column)?;
        let mut wheres = self.where_clauses;

        if let Some(value) = value {
            let param_name = format!("__PARAM_{}", self.params.len());

            let param = Table::get_parameter(value, &param_name)?;

            let mut required_params = self.params;
            required_params.push(param);

            wheres.push(format!("{} = @{}", column, param_name));

            return Ok(Self {
                where_clauses: wheres,
                params: required_params,
                ..self
            });
        }

        wheres.push(format!("{} is NULL", column));
        Ok(Self {
            where_clauses: wheres,
            ..self
        })
    }

    pub fn set_limit(self, limit: u32) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }
    //endregion

    //region build query
    fn build_where_string(&self) -> String {
        let mut where_string = String::new();
        if !self.where_clauses.is_empty() {
            where_string.push_str(" WHERE ");
            where_string.push_str(&self.where_clauses.join(" AND "));
        }
        where_string
    }
    fn build_order_by_string(&self) -> String {
        let mut order_by_string = String::new();
        if !self.order_by.is_empty() {
            order_by_string.push_str(" ORDER BY ");
            order_by_string.push_str(
                &self
                    .order_by
                    .iter()
                    .map(|(column, direction)| format!("{} {}", column, direction.to_query_str()))
                    .collect::<Vec<String>>()
                    .join(", "),
            );
        }
        order_by_string
    }
    fn build_limit_string(&self) -> String {
        let mut limit_string = String::new();
        if let Some(limit) = self.limit {
            limit_string.push_str(" LIMIT ");
            limit_string.push_str(&limit.to_string());
        }
        limit_string
    }
    //endregion
}

impl<Table: BigQueryTable + Default, QueryType: HasQueryType, Client: Default>
QueryBuilder<Table, QueryType, Client, QueryWasNotBuilt, NoStartingData>
{
    pub fn set_data(
        self,
        data: Table,
    ) -> QueryBuilder<Table, QueryType, Client, QueryWasNotBuilt, HasStartingData<Table>> {
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

impl<Table: BigQueryTable, Client: Default, StartingData: Default>
QueryBuilder<Table, QueryTypeNoType, Client, QueryWasNotBuilt, StartingData>
{
    pub fn select() -> QueryBuilder<Table, QueryTypeSelect, NoClient, QueryWasNotBuilt, StartingData>
    {
        QueryBuilder {
            query: String::from("SELECT "),
            ..Default::default()
        }
    }
    pub fn insert() -> QueryBuilder<Table, QueryTypeInsert, NoClient, QueryWasNotBuilt, StartingData>
    {
        QueryBuilder {
            query: String::from("INSERT INTO "),
            ..Default::default()
        }
    }
}

impl<Table: BigQueryTable + Default + Debug>
QueryBuilder<Table, QueryTypeInsert, HasClient, QueryWasNotBuilt, HasStartingData<Table>>
{
    pub fn build_query(
        self,
    ) -> Result<
        QueryBuilder<Table, QueryTypeInsert, HasClient, QueryWasBuilt, HasStartingData<Table>>,
    > {
        trace!("build_query: insert: {:?}", self);
        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        let fields = self.get_fields_string();
        let values = self.get_values_params_string()?;
        let params = &self.params;
        log::warn!("params are not used in insert query: {:?}", params);
        let mut params = vec![];
        let local_fields = Table::get_query_fields(true);
        let starting_data = &self.starting_data.0;
        for (local_field_name, _) in local_fields {
            let para = Table::get_parameter_from_field(starting_data, &local_field_name)?;
            params.push(para);
        }

        let query = format!(
            "insert into {} ({}) values({})",
            table_identifier, fields, values
        );
        Ok(QueryBuilder {
            query,
            params,
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
        let values = self.get_value_parameter_names()?;
        Ok(values
            .iter()
            .map(|v| format!("@{}", v))
            .collect::<Vec<String>>()
            .join(", "))
    }

    fn get_value_parameter_names(&self) -> Result<Vec<String>> {
        let mut values = self.get_sorted_selected_fields();
        let res = values
            .iter_mut()
            .map(|(field, _)| Table::get_field_param_name(field))
            .collect::<Result<Vec<String>>>()?;
        Ok(res)
    }
}

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

impl<Table: BigQueryTable + Debug, StartingData: Debug>
QueryBuilder<Table, QueryTypeSelect, HasClient, QueryWasNotBuilt, StartingData>
{
    pub fn build_query(
        self,
    ) -> QueryBuilder<Table, QueryTypeSelect, HasClient, QueryWasBuilt, StartingData> {
        trace!("build_query: select: {:?}", self);

        let table_identifier = Table::get_table_identifier_from_client(&self.client.0);
        let fields_str = self.get_fields_string();
        let where_clause = self.build_where_string();
        let order_by_clause = self.build_order_by_string();
        let limit_clause = self.build_limit_string();
        let query = format!(
            "SELECT {} FROM {}{}{}{}",
            fields_str, table_identifier, where_clause, order_by_clause, limit_clause
        );
        QueryBuilder {
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
        }
    }
}

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
}

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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use serde_json::Value;

    use super::*;

    #[derive(Debug, Default, Clone)]
    struct TestTable {
        client: BigqueryClient,
        row_id: i64,
        info1: Option<String>,
        info3: Option<String>,
        info4i: Option<i32>,
        info4b: Option<bool>,
    }

    #[async_trait::async_trait]
    impl BigQueryTableBase for TestTable {
        fn get_all_params(&self) -> Result<Vec<QueryParameter>> {
            todo!()
        }

        fn get_parameter_from_field(&self, field_name: &str) -> Result<QueryParameter> {
            todo!()
        }

        fn get_table_name() -> String {
            todo!()
        }

        fn get_client(&self) -> &BigqueryClient {
            todo!()
        }

        fn set_client(&mut self, client: BigqueryClient) {
            todo!()
        }

        fn get_pk_field_name() -> String {
            todo!()
        }

        fn get_pk_db_name() -> String {
            todo!()
        }

        fn get_pk_value(&self) -> &(dyn BigDataValueType + Send + Sync) {
            todo!()
        }

        fn get_query_fields(include_pk: bool) -> HashMap<String, String> {
            todo!()
        }

        async fn reload(&mut self) -> Result<()> {
            todo!()
        }

        fn set_field_value(&mut self, field_name: &str, value: &Value) -> Result<()> {
            todo!()
        }

        fn get_field_value(&self, field_name: &str) -> Result<Value> {
            todo!()
        }

        fn new_from_query_result_row(
            client: BigqueryClient,
            row: &HashMap<String, Value>,
        ) -> Result<Self>
            where
                Self: Sized,
        {
            todo!()
        }

        async fn insert(&mut self) -> Result<()> {
            todo!()
        }

        async fn update(&mut self) -> Result<()> {
            todo!()
        }
    }

    impl TestTable {
        fn select() -> QueryBuilder<Self, QueryTypeSelect, NoClient, QueryWasNotBuilt, NoStartingData>
        {
            QueryBuilder::<Self, QueryTypeNoType, NoClient, QueryWasNotBuilt, NoStartingData>::select()
        }
        fn insert() -> QueryBuilder<Self, QueryTypeInsert, NoClient, QueryWasNotBuilt, HasStartingData<Self>>
        {
            QueryBuilder::<Self, QueryTypeNoType, NoClient, QueryWasNotBuilt, HasStartingData<Self>>::insert()
        }
    }

    #[tokio::test]
    async fn test1() {
        let client = BigqueryClient::new("test", "", None).await.unwrap();
        let query_builder = TestTable::select().with_client(client.clone());
        println!("{:?}", query_builder);
        let query_builder = query_builder.build_query();

        println!("query: {:?}", query_builder);
        let query_builder = TestTable::insert();
        println!("{:?}", query_builder);
        let query_builder = query_builder.with_client(client);
        let query_builder = query_builder
            .build_query()
            .expect("build of insert query failed");
        let result = query_builder.clone().run().await;
        println!("query: {:?}", query_builder);
    }
}
