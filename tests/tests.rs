use log::LevelFilter;
use nameof::name_of;

use google_bigquery_v2::data::query_builder::QueryResultType;
use google_bigquery_v2::prelude::*;

#[derive(BigDataTableDerive, Debug, Default, Clone)]
#[db_name("Infos")]
pub struct DbInfos {
    #[client]
    client: BigqueryClient,
    #[primary_key]
    #[db_name("Id")]
    row_id: i64,
    info1: Option<String>,
    #[db_name("info")]
    info2: Option<String>,
    info3: Option<String>,
    info4i: Option<i32>,
    #[db_name("yes")]
    info4b: Option<bool>,
}

#[tokio::test]
async fn test1() {
    init_logger();
    let client = get_test_client().await;
    let query_builder = DbInfos::select().with_client(client.clone());
    debug!("{:?}", query_builder);
    let query_builder = query_builder.build_query().unwrap();

    debug!("query: {:?}", query_builder);
    let result = query_builder.clone().run().await;
    debug!("select result: {:?}", result);
    let sample_data = DbInfos {
        client: client.clone(),
        row_id: 9999,
        info1: Some("test1".to_string()),
        info2: None,
        info3: Some("test3".to_string()),
        info4i: Some(1),
        info4b: Some(true),
    };

    let query_builder = DbInfos::insert();
    debug!("{:?}", query_builder);
    let query_builder = query_builder.with_client(client.clone());
    let query_builder = query_builder.set_data(sample_data.clone());
    let query_builder = query_builder.build_query().expect("query builder failed");
    debug!("query: {:?}", query_builder);
    let result = query_builder.clone().run().await;
    debug!("query: {:?}", query_builder);
    debug!("result: {:?}", result);

    let query_builder = DbInfos::delete()
        .with_client(client.clone())
        .set_data(sample_data);
    let query_builder = query_builder.build_query().expect("query builder failed");
    debug!("query: {:?}", query_builder);
    let result = query_builder.clone().run().await;
    debug!("query: {:?}", query_builder);
    debug!("result: {:?}", result);
    result.expect("result is not ok");
}

#[tokio::test]
async fn test_save() {
    init_logger();
    let client = get_test_client().await;
    let mut entry = DbInfos::get_by_pk(client.clone(), &123123)
        .await
        .expect("get_by_pk failed");
    entry.info1 = Some("test1".to_string());
    entry.info2 = Some("test2".to_string());
    entry.info3 = None;
    entry.info4b = Some(true);
    let info4i = entry.info4i;
    debug!("entry: {:?}", entry);
    debug!("========================================================================");
    debug!("starting save");
    debug!("========================================================================");
    entry.save().await.expect("save failed");
    debug!("========================================================================");
    debug!("save done");
    debug!("========================================================================");
    let info1 = entry.info1.clone().unwrap();
    entry.info1 = Some("0987654321".to_string());

    debug!("========================================================================");
    debug!("starting reload");
    debug!("========================================================================");
    entry.reload().await.expect("reload failed");
    debug!("========================================================================");
    debug!("reload done");
    debug!("========================================================================");
    assert_eq!(info1, entry.info1.unwrap(), "reload failed");
    assert_eq!(
        None, entry.info3,
        "Info 3 should be set to None before the save happened"
    );
    assert_eq!(
        info4i, entry.info4i,
        "Info 4i should not have changed between the load from pk and the reload"
    );
}

#[tokio::test]
async fn test_get_table_name() {
    init_logger();
    let pk = DbInfos::get_table_name();
    debug!("table name: {}", pk);
    assert_eq!("Infos", pk, "table name is not correct")
}

#[tokio::test]
async fn test_get_query_fields() {
    init_logger();
    let fields = DbInfos::get_query_fields(true);
    debug!("fields: {:?}", fields);
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
    let query_builder = DbInfos::select().with_client(client);
    let query_builder = query_builder
        .add_where_eq::<String>(name_of!(info1 in DbInfos), None)
        .unwrap()
        .add_where_eq(name_of!(info3 in DbInfos), Some(&"cc".to_string()))
        .unwrap()
        .add_order_by(name_of!(info2 in DbInfos), OrderDirection::Ascending);
    let query_string = query_builder
        .clone()
        .build_query()
        .unwrap()
        .get_query_string()
        .to_string();
    let expected_query_string =
        "SELECT info1, info, info3, yes, info4i, Id FROM `testrustproject-372221.test1.Infos` WHERE info1 is NULL AND info3 = @__PARAM_0 ORDER BY info ASC".to_string()
        ;
    debug!("query   : {}", query_string);
    debug!("expected: {}", expected_query_string);
    debug!("request: {:?}", query_builder.clone().build_query());

    assert_eq!(query_string, expected_query_string);
    // assert_eq!(
    //     query_builder
    //         .clone()
    //         .build_query_request()
    //         .query_parameters
    //         .unwrap()
    //         .len(),
    //     1
    // );
    let res = query_builder
        .clone()
        .build_query()
        .unwrap()
        .run()
        .await
        .unwrap();
    debug!("res: {:?}", res);
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
    let q = DbInfos::select()
        .with_client(client)
        .add_order_by(name_of!(row_id in DbInfos), OrderDirection::Descending)
        .build_query()
        .unwrap()
        .run()
        .await
        .unwrap();
    let q = match q {
        QueryResultType::WithRowData(q) => q,
        QueryResultType::WithoutRowData(e) => panic!("no data: {:?}", e),
    };
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
    let q: Vec<DbInfos> = DbInfos::select()
        .with_client(client)
        .set_limit(1)
        .build_query()
        .unwrap()
        .run()
        .await
        .unwrap()
        .expect_with_data("no data");
    assert_eq!(q.len(), 1);
}

#[tokio::test]
async fn test_upsert() {
    init_logger();
    let client = get_test_client().await;
    let mut local = DbInfos {
        client: client.clone(),
        row_id: 1923,
        info1: None,
        info2: None,
        info3: None,
        info4i: None,
        info4b: None,
    };
    local.upsert().await.expect("could not perform upsert!");
    DbInfos::delete()
        .with_client(client)
        .set_data(local)
        .build_query()
        .expect("could not build delete")
        .run()
        .await
        .expect("could not run delete")
        .expect_without_data("delete should not return any data");
}

#[test]
fn test_empty_client() {
    let empty_client = BigqueryClient::empty();
    debug!("empty client: {:?}", empty_client);
}

fn init_logger() {
    let global_level = LevelFilter::Info;
    let own_level = LevelFilter::Trace;
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(global_level)
        .filter_module("google_bigquery_v2", own_level)
        .filter_module("google_bigquery_v2_derive", own_level)
        .filter_module("tests", own_level)
        .try_init();
}
