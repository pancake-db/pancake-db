use pancake_db_idl::schema::Schema;

use super::traits::{CacheData, Metadata};

impl Metadata<String> for Schema {
  fn relative_path(table_name: &String) -> String {
    return format!("{}/schema.json", table_name);
  }
}

pub type SchemaCache = CacheData<String, Schema>;

impl SchemaCache {
  pub async fn assert(&self, table_name: &str, schema: &Schema) -> Result<(), &'static str> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let table_name_string = table_name.to_string();
    if !map.contains_key(&table_name_string) {
      map.insert(
        table_name.to_string(),
        Schema::load(&self.dir, &table_name_string).await.map(|s| *s)
      );
    }

    return match map.get(table_name).unwrap() {
      Some(existing_schema) => {
        if existing_schema == schema {
          Ok(())
        } else {
          Err("existing schema does not match")
        }
      },
      None => {
        map.insert(table_name.to_string(), Some(schema.clone()));
        schema.overwrite(&self.dir, &table_name_string).await
      }
    };
  }
}
