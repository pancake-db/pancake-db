# [PancakeDB](https://pancakedb.com)

PancakeDB is an event ingestion solution.
It is simple to set up and computationally cheaper than alternatives.
A 1-node instance of PancakeDB can handle >10k writes per second.
Reading from PancakeDB into Spark is even faster than reading from Parquet files.
PancakeDB is causally consistent, so the data you write is available in real time.

## Getting Started

To start a simple 1-node deployment, you can run
```bash
git clone https://github.com/pancake-db/pancake-db && cd pancake-db
docker build . -t pancake-db:latest # will take several minutes
docker run --rm -p 3841:3841 -v $DATA_DIRECTORY:/pancake_db_data pancake-db:latest
```

Now you can write data either via HTTP or one of the client libraries. E.g.
```
# create a table
curl -XPOST -H ContentType:application/json localhost:3841/rest/create_table -d '{
  "tableName": "my_purchase_table",
  "schema": {
    "partitioning": {
      "day": {"dtype": "TIMESTAMP_MINUTE"}
    },
    "columns": {
      "user_id": {"dtype": "STRING"},
      "cents_amount": {"dtype": "INT64"}
    }
  }
}'

# write a row
curl -XPOST -H ContentType:application/json localhost:3841/rest/write_to_partition_simple -d '{
  "table_name": "my_purchase_table",
  "partition": {
    "day": {"timestamp": "2022-01-01T00:00:00Z"}
  },
  "rows": [{
    "user_id": "abc",
    "cents_amount": 1234
  }]
}'
```

If you have Spark installed, you can set up a project depending on [the PancakeDB Spark connector]() and access the tables efficiently.
For instance,
```
spark-shell --jars $MY_SPARK_PROJECT_UBERJAR

scala> val t = spark.read.format("pancake").option("host", "localhost").option("port", 3841).option("table_name", "my_purchase_table").load()

scala> t.show()
+-------+------------+-------------------+                                      
|user_id|cents_amount|                day|
+-------+------------+-------------------+
|    abc|        1234|2021-12-31 19:00:00|
+-------+------------+-------------------+


scala> t.createOrReplaceTempView("t")

scala> spark.sql("select count(*) from t").show()
+--------+
|count(1)|
+--------+
|       1|
+--------+
```

## Contributing

To get involved, [join the Discord](https://discord.gg/f6eRXgMP8w) or submit a GitHub issue.
