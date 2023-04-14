use log::{debug, info, LevelFilter};
use nameof::name_of;

use google_bigquery_v2::data::query_builder::{
    HasStartingData, NoClient, NoStartingData, QueryBuilder, QueryTypeInsert, QueryTypeNoType,
    QueryTypeSelect, QueryWasNotBuilt,
};
use google_bigquery_v2::prelude::*;

#[derive(BigDataTableDerive, Debug, Default, Clone)]
#[db_name("Infos")]
pub struct DbInfos {
    #[client]
    client: BigqueryClient,
    #[primary_key]
    #[db_name("Id")]
    row_id: i64,
    info1: Option::<String>,
    #[db_name("info")]
    info2: Option::<String>,
    info3: Option::<String>,
    info4i: Option::<i32>,
    #[db_name("yes")]
    info4b: Option::<bool>,
}
pub struct DbInfos2{
    client: BigqueryClient,
    row_id: i64,
    info1: Option::<String>,
    info2: Option<String>,
    info3: Option<String>,
    info4i: Option<i32>,
    info4b: Option<bool>,
}

//TODO: outsource this impl into the derive
impl DbInfos {
    fn select() -> QueryBuilder<Self, QueryTypeSelect, NoClient, QueryWasNotBuilt, NoStartingData> {
        QueryBuilder::<Self, QueryTypeNoType, NoClient, QueryWasNotBuilt, NoStartingData>::select()
    }
    fn insert() -> QueryBuilder<Self, QueryTypeInsert, NoClient, QueryWasNotBuilt, NoStartingData> {
        QueryBuilder::<Self, QueryTypeNoType, NoClient, QueryWasNotBuilt, NoStartingData>::insert()
    }
}

#[tokio::test]
async fn test1() {
    init_logger();
    let client = get_test_client().await;
    let query_builder = DbInfos::select().with_client(client.clone());
    debug!("{:?}", query_builder);
    let query_builder = query_builder.build_query();

    debug!("query: {:?}", query_builder);
    let result = query_builder.clone().run().await;
    debug!("select result: {:?}", result);
    let sample_data = DbInfos {
        client: client.clone(),
        row_id: 1,
        info1: Some("test1".to_string()),
        info2: Some("test2".to_string()),
        info3: Some("test3".to_string()),
        info4i: Some(1),
        info4b: Some(true),
    };

    let query_builder = DbInfos::insert();
    println!("{:?}", query_builder);
    let query_builder = query_builder.with_client(client);
    let query_builder = query_builder.set_data(sample_data);
    let query_builder = query_builder.build_query().expect("query builder failed");
    println!("query: {:?}", query_builder);
    let result = query_builder.clone().run().await;
    println!("query: {:?}", query_builder);
    println!("result: {:?}", result);
}

#[tokio::test]
async fn test_get_table_name() {
    init_logger();
    let pk = DbInfos::get_table_name();
    log::debug!("table name: {}", pk);
    assert_eq!("Infos", pk, "table name is not correct")
}

#[tokio::test]
async fn test_get_query_fields() {
    init_logger();
    let fields = DbInfos::get_query_fields(true);
    log::debug!("fields: {:?}", fields);
    assert_eq!(6, fields.len(), "fields length is not correct");
    assert_eq!("Id", fields.get("row_id").unwrap(),);
    assert_eq!("info1", fields.get("info1").unwrap(),);
    assert_eq!("info", fields.get("info2").unwrap());
    assert_eq!("info3", fields.get("info3").unwrap());
    assert_eq!("info4i", fields.get("info4i").unwrap());
    assert_eq!("yes", fields.get("info4b").unwrap());
}

#[tokio::test]
async fn test_query_builder_1() {
    init_logger();
    let client = get_test_client().await;
    let query_builder: BigQueryBuilder<DbInfos> = DbInfos::query(client);
    let query_builder: BigQueryBuilder<DbInfos> = query_builder
        .add_where_eq::<String>(name_of!(info1 in DbInfos), None)
        .unwrap()
        .add_where_eq(name_of!(info3 in DbInfos), Some(&"cc".to_string()))
        .unwrap()
        .add_order_by(name_of!(info2 in DbInfos), OrderDirection::Ascending);
    let query_string = query_builder.clone().build_query_string();
    let expected_query_string = String::from(
        "SELECT Id, info, info1, info3, info4i, yes \
    FROM `testrustproject-372221.test1.Infos` \
    WHERE info1 is NULL AND info3 = @__PARAM_0 \
    ORDER BY info ASC LIMIT 1000",
    );
    log::debug!("query   : {}", query_string);
    log::debug!("expected: {}", expected_query_string);
    log::debug!("request: {:?}", query_builder.clone().build_query_request());

    assert_eq!(query_string, expected_query_string);
    assert_eq!(
        query_builder
            .clone()
            .build_query_request()
            .query_parameters
            .unwrap()
            .len(),
        1
    );
    let res = query_builder.clone().run().await.unwrap();
    log::debug!("res: {:?}", res);
}

async fn get_test_client() -> BigqueryClient {
    BigqueryClient::new("testrustproject-372221", "test1", None)
        .await
        .unwrap()
}

#[tokio::test]
async fn simple_query() {
    init_logger();
    let client = get_test_client().await;
    let q: Vec<DbInfos> = DbInfos::query(client)
        .add_order_by(name_of!(row_id in DbInfos), OrderDirection::Descending)
        .run()
        .await
        .unwrap();
    let mut last_num = 999999999999999999;
    for line in q {
        info!("line: {:?}", line);
        debug!("row_id > last: {} <= {}", line.row_id, last_num);
        assert!(line.row_id <= last_num);
        last_num = line.row_id;
    }
}

#[tokio::test]
async fn test_select_limit_1() {
    init_logger();
    let client = get_test_client().await;
    let q: Vec<DbInfos> = DbInfos::query(client).set_limit(1).run().await.unwrap();
    assert_eq!(q.len(), 1);
}

fn init_logger() {
    let global_level = LevelFilter::Info;
    let own_level = LevelFilter::Debug;
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(global_level)
        .filter_module("google_bigquery_v2", own_level)
        .filter_module("google_bigquery_v2_derive", own_level)
        .filter_module("tests", own_level)
        .try_init();
}
