use std::fmt::Debug;

use chrono::{NaiveDateTime, Utc};
use log::{trace, warn};
use serde_json::{value, Value};

use crate::prelude::*;

pub trait ConvertBigQueryParams {
    fn from_param(value: &Value) -> Result<Self>
    where
        Self: Sized;
    fn to_param(&self) -> Value;
}

impl ConvertBigQueryParams for i64 {
    fn from_param(value: &Value) -> Result<Self> {
        let string: String = serde_json::from_value(value.clone())?;
        Ok(string.parse()?)
    }
    fn to_param(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

impl ConvertBigQueryParams for i32 {
    fn from_param(value: &Value) -> Result<Self> {
        let string: String = serde_json::from_value(value.clone())?;
        Ok(string.parse()?)
    }
    fn to_param(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

impl ConvertBigQueryParams for bool {
    fn from_param(value: &Value) -> Result<Self> {
        let value: String = serde_json::from_value(value.clone())?;
        match value.as_str() {
            "TRUE" => Ok(true),
            "true" => Ok(true),
            "FALSE" => Ok(false),
            "false" => Ok(false),
            invalid => Err(format!("Invalid value for bool: '{}'", invalid).into()),
        }
    }
    fn to_param(&self) -> Value {
        match self {
            true => serde_json::to_value("TRUE").unwrap(),
            false => serde_json::to_value("FALSE").unwrap(),
        }
    }
}

impl ConvertBigQueryParams for String {
    fn from_param(value: &Value) -> Result<Self> {
        let string: String = serde_json::from_value(value.clone())?;
        Ok(string.parse()?)
    }
    fn to_param(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

impl ConvertBigQueryParams for f64 {
    fn from_param(value: &Value) -> Result<Self> {
        Ok(serde_json::from_value(value.clone())?)
    }
    fn to_param(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

impl ConvertBigQueryParams for chrono::DateTime<Utc> {
    fn from_param(value: &Value) -> Result<Self> {
        trace!(
            "ConvertValueToBigqueryParamValue::from_param DateTime<Utc> -> in:  {:?}",
            value
        );
        let value: String = serde_json::from_value(value.clone())?;
        let value = value.replace("T", " ").replace("Z", "");
        let value = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")?;
        let time = chrono::DateTime::<Utc>::from_utc(value, Utc);
        trace!(
            "ConvertValueToBigqueryParamValue::from_param DateTime<Utc> -> out: {:?}",
            time
        );
        Ok(time)
    }
    fn to_param(&self) -> Value {
        trace!(
            "ConvertValueToBigqueryParamValue::to_param DateTime<Utc> -> in:  {:?}",
            self
        );
        let value: String = self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let value: String = value.replace("Z", "").replace("T", " ");
        trace!(
            "ConvertValueToBigqueryParamValue::to_param DateTime<Utc> -> out: {:?}",
            value
        );
        serde_json::to_value(value).unwrap()
    }
}

impl<T: ConvertBigQueryParams + Debug> ConvertBigQueryParams for Option<T> {
    fn from_param(value: &Value) -> Result<Self>
    where
        Self: Sized,
    {
        trace!(
            "ConvertValueToBigqueryParamValue::from_param Option<T>: {:?}",
            value
        );
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(T::from_param(value)?)),
        }
    }

    fn to_param(&self) -> Value {
        trace!(
            "ConvertValueToBigqueryParamValue::to_param Option<T>: {:?}",
            self
        );
        match self {
            Some(value) => value.to_param(),
            None => Value::Null,
        }
    }
}

pub fn convert_value_to_string(value: Value) -> Result<String> {
    trace!(
        "ConvertValueToBigqueryParamValue::convert_value_to_string: {:?}",
        value
    );
    return if value.is_string() {
        trace!("ConvertValueToBigqueryParamValue::convert_value_type_to_bigquery_type: String");
        Ok(value::from_value(value)?)
    } else {
        warn!("Unknown type: {:?}", value);
        if value == Value::Null {
            return Err("Value is Null".into());
        }
        //TODO: check if this is correct with for example 'DATETIME' values
        // Err(format!("Unknown type: {:?}", value).into())
        let string = value.to_string();
        Ok(string)
    };
}
