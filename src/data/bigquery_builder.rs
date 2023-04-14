use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use google_bigquery2::api::{
    QueryParameter, QueryParameterType, QueryParameterValue, QueryRequest,
};
use log::{debug, trace};
use serde_json::Value;

use crate::client::BigqueryClient;
use crate::data::BigQueryTable;
use crate::data::param_conversion::BigDataValueType;
use crate::prelude::*;

//region OrderDirection


//endregion

//region BigQueryBuilderAvailable

// pub trait BigQueryBuilderAvailable<'a, Table> {
//     fn query(client: &'a BigqueryClient) -> BigQueryBuilder<'a, Table>;
// }
//
// impl<'a, Table> BigQueryBuilderAvailable<'a, Table> for Table
//     where
//         Table: BigQueryTable<'a>,
// {
// }

//endregion
