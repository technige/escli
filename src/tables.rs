use std::collections::HashMap;

use ascii_table::AsciiTable;
use serde_json::{json, Value};

pub struct Table {
    column_names: Vec<String>,
    rows: Vec<HashMap<String, Value>>,
}

impl Table {
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
        self.rows.push(row.to_owned());
    }

    pub fn print(&self) {
        let mut table = AsciiTable::default();
        for (i, name) in self.column_names.iter().enumerate() {
            table.column(i).set_header(name);
        }
        let mut data: Vec<Vec<String>> = vec![];
        for row in self.rows.iter() {
            let mut row_data: Vec<String> = vec![];
            for column_name in self.column_names.iter() {
                let value = row.get(column_name).unwrap_or_else(|| &json!(null));
                match value {
                    Value::String(string_value) => {
                        row_data.push(string_value.to_string());
                    }
                    _ => {
                        row_data.push(value.to_string());
                    }
                }
            }
            data.push(row_data);
        }
        table.print(data);
    }
}
