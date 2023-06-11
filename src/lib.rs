
use fluvio_smartmodule::{
    smartmodule, Record, RecordData, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError},
};

use once_cell::sync::OnceCell;

use csv::{ReaderBuilder};

use std::collections::HashMap;

use chrono::{Utc,TimeZone};

fn parse_timestamp(timestamp: &str) -> i64 {
    let timestamp = timestamp.parse::<f64>().unwrap();
    let dt = Utc.timestamp_millis((timestamp * 1000.0) as i64);
    dt.timestamp_millis()+1686505266630
}

static TIMESTAMP: OnceCell<String> = OnceCell::new();

/// Expend a JSON aggregate Record containing arrays into a list of individual JSON Records.
fn csv_to_records(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {

    let param=TIMESTAMP.get().unwrap();
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(record.value.as_ref());
    type CSVRecord = HashMap<String, String>;
    let mut rows = Vec::new();
    for result in reader.deserialize() {
        let mut record: CSVRecord = result?;
        println!("Record {:?}", record);
        let timestamp=&record.remove(param).unwrap();
        let timestamp=parse_timestamp(timestamp.trim());
        for (key, value) in record {
            let mut row = HashMap::new();
            row.insert("key".to_string(), key);
            row.insert("value".to_string(), value);
            row.insert("timestamp".to_string(), timestamp.to_string());
            rows.push(row);
        }
    }

        // Convert each JSON value from the array back into a JSON string
        let strings: Vec<String> = rows
        .into_iter()
        .map(|value| serde_json::to_string(&value))
        .collect::<core::result::Result<_, _>>()?;

    // Create one record from each JSON string to send
    let kvs: Vec<(Option<RecordData>, RecordData)> = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect();

    Ok(kvs)
}

#[smartmodule(array_map)]
pub fn array_map(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a JSON array with any kind of values inside
    let result = csv_to_records(record)?;
    Ok(result)
}


#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(key) = params.get("timestamp") {
        TIMESTAMP
            .set(key.clone())
            .map_err(|err| eyre!("failed setting key: {:#?}", err))
    } else {
        Err(SmartModuleInitError::MissingParam("timestamp".to_string()).into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_array_map() {
        let input = include_str!("../test-data/input.csv");
        let result = array_map(&Record::new(input)).unwrap();

        let expected = include_str!("../test-data/output.json");
        let expected_value:Vec<(Option<RecordData>, RecordData)> = serde_json::from_str(expected).unwrap();
        assert_eq!(expected_value, result);
    }
}