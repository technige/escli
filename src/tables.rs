use std::collections::HashMap;

use serde_json::{json, Value};
use tabled::builder::Builder;

pub struct TabularData {
    column_names: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl TabularData {
    pub fn new() -> Self {
        Self {
            column_names: vec![],
            rows: vec![],
        }
    }

    pub fn push_row(&mut self, row: &HashMap<String, Value>) {
        for (key, _value) in row.into_iter() {
            if !self.column_names.contains(key) {
                self.column_names.push(key.to_owned());
            }
        }
        let mut string_values: Vec<String> = vec![];
        for column_name in self.column_names.iter() {
            let value = row.get(column_name).unwrap_or_else(|| &json!(null));
            match value {
                Value::String(string_value) => {
                    string_values.push(string_value.to_string());
                }
                _ => {
                    string_values.push(value.to_string());
                }
            }
        }
        self.rows.push(string_values);
    }

    pub fn to_table(&self) -> tabled::Table {
        let mut builder = Builder::default();
        builder.push_record(self.column_names.clone());
        for row in self.rows.iter() {
            builder.push_record(row)
        }
        builder.build()
    }
}
