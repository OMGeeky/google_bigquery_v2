use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use google_bigquery2::api::{
    QueryParameter, QueryParameterType, QueryParameterValue, QueryRequest,
};
use log::{debug, log, trace};
use serde_json::Value;

use crate::client::BigqueryClient;
use crate::data::BigQueryTable;
use crate::data::param_conversion::BigDataValueType;
use crate::prelude::*;

//region OrderDirection

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

//endregion

//region BigQueryBuilder

#[derive(Debug, Clone)]
pub struct BigQueryBuilder<'a, Table> {
    client: Option<&'a BigqueryClient>,
    required_params: Vec<QueryParameter>,
    selected_fields: Option<Vec<String>>,
    wheres: Vec<String>,
    limit: Option<usize>,
    order_bys: Vec<(String, OrderDirection)>,

    _table_type_marker: PhantomData<Vec<Table>>,
}

impl<'a, Table> BigQueryBuilder<'a, Table>
    where
        Table: BigQueryTable<'a>,
{
    //region build methods

    pub async fn run(self) -> Result<Vec<Table>> {
        trace!("BigQueryBuilder::run()");
        //TODO: maybe return an iterator instead of a vector.
        //      this would allow for lazy loading of the data.
        //      it would also make it possible that additional
        //      data is loaded (if the set limit is higher than
        //      the number of rows returned)
        let client = self.client.unwrap();
        let fields = self.get_sorted_selected_fields();
        let req = self.build_query_request();

        debug!("req: {:?}", req);
        let (res, query_res) = client
            .get_client()
            .jobs()
            .query(req, client.get_project_id())
            .doit()
            .await?;

        if res.status() != 200 {
            return Err(format!("Wrong status code returned! ({})", res.status()).into());
        }

        let query_res = query_res.rows.unwrap();
        println!("query_res: {:?}", query_res);
        let mut result: Vec<Table> = Vec::new();
        for row in query_res {
            let row = row.f.unwrap();
            let mut row_data: HashMap<String, Value> = HashMap::new();
            for (i, field) in row.into_iter().enumerate() {
                let field = field.v.unwrap_or(Value::Null);
                println!("{}: {}", fields[i], field);
                row_data.insert(fields[i].clone(), field);
            }
            let data = Table::new_from_query_result_row(client, &row_data)?;
            result.push(data);
        }

        return Ok(result);
    }

    pub fn build_query_request(self) -> QueryRequest {
        QueryRequest {
            query: Some(self.build_query_string()),
            query_parameters: Some(self.required_params),
            use_legacy_sql: Some(false),
            /*TODO: is this line needed?: use_legacy_sql: Some(false),*/
            ..Default::default()
        }
    }
    pub fn build_query_string(&self) -> String {
        let where_clause = if self.wheres.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.wheres.join(" AND "))
        };
        let order_by_clause = if self.order_bys.is_empty() {
            String::new()
        } else {
            format!("ORDER BY {}", self.order_bys
                .iter()
                .map(|(key, dir)| format!("{} {}", key, dir.to_query_str()))
                .collect::<Vec<String>>()
                .join(", "))
        };
        format!(
            "SELECT {} FROM {} {} {} LIMIT {}",
            self.get_sorted_selected_fields()
                .join(", "),
            Table::get_table_identifier_from_client(self.client.unwrap()),
            where_clause,
            order_by_clause,
            self.limit.unwrap_or(1000)
        )
    }

    //endregion


    //region add content

    fn set_select_fields(self, fields: Vec<String>) -> Result<Self> {
        //TODO: this method probably does not work since the logic does
        //      not work if (at least the required) fields are not selected
        //      since the parser will not be able to create the struct instance.
        let selected_fields = self.selected_fields;
        let mut selected_fields = match selected_fields {
            Some(selected_fields) => selected_fields,
            None => Vec::new(),
        };

        for field in fields {
            let field_name = Table::get_field_db_name(&field)
                .map_err(|e| format!("Error while selecting field '{}': {}", field, e))?;
            selected_fields.push(field_name);
        }

        Ok(Self {
            selected_fields: Some(selected_fields),
            ..self
        })
    }

    pub fn add_where_eq<T>(self, column: &str, value: Option<&T>) -> Result<Self>
        where
            T: BigDataValueType + Debug,
    {
        let column = Table::get_field_db_name(column)?;
        let mut wheres = self.wheres;

        if let Some(value) = value {
            let param_name = format!("__PARAM_{}", self.required_params.len());

            let param = get_parameter(value, &param_name);

            let mut required_params = self.required_params;
            required_params.push(param);

            wheres.push(format!("{} = @{}", column, param_name));

            return Ok(Self {
                wheres,
                required_params,
                ..self
            });
        }

        wheres.push(format!("{} is NULL", column));
        Ok(Self { wheres, ..self })
    }

    pub fn add_order_by(self, column: &str, direction: OrderDirection) -> Self {
        let column = Table::get_field_db_name(column).unwrap();
        let mut order_bys = self.order_bys;
        order_bys.push((column.to_string(), direction));
        Self { order_bys, ..self }
    }
    //endregion

    fn get_sorted_selected_fields(&self) -> Vec<String> {
        trace!("get_sorted_selected_fields()");
        let mut fields: Vec<String> = match &self.selected_fields {
            Some(fields) => fields.clone(),
            None => {
                Table::get_query_fields(true)
                    .into_iter()
                    .map(|f| f.1)//get the db name
                    .collect()
            }
        };
        println!("fields: {:?}", fields);
        fields.sort();
        fields
    }
}
//region implement some convenience traits for BigQueryBuilder

impl<'a, Table> Default for BigQueryBuilder<'a, Table> {
    fn default() -> Self {
        Self {
            client: None,
            required_params: vec![],
            selected_fields: None,
            wheres: vec![],
            limit: None,
            order_bys: vec![],
            _table_type_marker: PhantomData,
        }
    }
}

impl<'a, Table: BigQueryTable<'a>> Display for BigQueryBuilder<'a, Table> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "BigQueryBuilder: {}\t\
        wheres: 'where {}'\t\
        order by's: 'order by {}'\t\
        limit: {:?}\t\
        params: {:?}",
            Table::get_table_name(),
            self.wheres.join(" AND "),
            self.order_bys
                .iter()
                .map(|(key, dir)| format!("{} {}", key, dir.to_query_str()))
                .collect::<Vec<String>>()
                .join(", "),
            self.limit,
            self.required_params
        ))
    }
}

//endregion

//endregion

fn get_parameter<T>(value: &T, param_name: &String) -> QueryParameter
    where
        T: BigDataValueType + Debug,
{
    let param_value = serde_json::from_value(value.to_param()).unwrap();
    let param_value = QueryParameterValue {
        value: Some(param_value),
        ..Default::default()
    };

    let param_type = T::convert_type_to_bigquery_type();
    let param_type = QueryParameterType {
        type_: Some(param_type),
        ..Default::default()
    };

    let param = QueryParameter {
        parameter_type: Some(param_type),
        parameter_value: Some(param_value),
        name: Some(param_name.clone()),
    };
    param
}

//region BigQueryBuilderAvailable

pub trait BigQueryBuilderAvailable<'a, Table> {
    fn query(client: &'a BigqueryClient) -> BigQueryBuilder<'a, Table>;
}

impl<'a, Table> BigQueryBuilderAvailable<'a, Table> for Table
    where
        Table: BigQueryTable<'a>,
{
    fn query(client: &'a BigqueryClient) -> BigQueryBuilder<'a, Table> {
        BigQueryBuilder {
            client: Some(client),
            ..Default::default()
        }
    }
}

//endregion
