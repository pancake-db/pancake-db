use serde::{Deserialize, Serialize};
use pancake_db_idl::schema::{Schema, ColumnMeta};

use super::traits::{CacheData, Metadata};

impl Metadata<String> for Schema {
  fn relative_path(table_name: &String) -> String {
    return format!("{}/schema.json", table_name);
  }
}

pub type SchemaCache = CacheData<String, Schema>;

impl SchemaCache {
  pub async fn get_all_table_names(&self) -> Vec<String> {
    let mux_guard = self.data.read().await;
    let map = &*mux_guard;
    return map.keys().map(|s| s.to_owned()).collect();
  }

  pub async fn assert(&self, table_name: &str, schema: &Schema) -> Result<(), &'static str> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let table_name_string = String::from(table_name);
    if !map.contains_key(&table_name_string) {
      Schema::load(&self.dir, &table_name_string).await.map(|existing_schema| {
        map.insert(String::from(table_name), *existing_schema);
      });
    }

    return match map.get(table_name) {
      Some(existing_schema) => {
        if existing_schema == schema {
          Ok(())
        } else {
          Err("existing schema does not match")
        }
      },
      None => {
        map.insert(String::from(table_name), schema.clone());
        schema.overwrite(&self.dir, &String::from(table_name)).await
      }
    };
  }
}
