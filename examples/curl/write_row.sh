curl -XPOST -H "Content-Type: application/json" http://localhost:1337/rest/write_to_partition -d '{"table_name": "asdf", "partition": [], "rows": [{"fields": [{"name": "i", "value": {"int64Val": 44}}, {"name": "s", "value": {"stringVal": "lets go"}}]}]}'
