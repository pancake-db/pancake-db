curl -XGET -H "Content-Type: application/json" http://localhost:1337/rest/read_segment_column -d '{"table_name": "asdf", "column_name": "s", "segment_id": "'$1'"}'
