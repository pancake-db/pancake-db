# python 3
# pip requirement: numpy, pyarrow
# assumes dictionary file located in 

import numpy as np
import pyarrow as pa
from pyarrow import parquet as pq
import pyarrow.csv
import os

n = 10 ** 6

words = [w.lower() for w in open('/usr/share/dict/words').read().split('\n')]

cols = {
  'int64': np.random.randint(0, 100, size=n),
  'float64': np.random.normal(size=n) + 100.0,
  'string': np.random.choice(words, size=n, replace=True),
  'bool': np.random.uniform(size=n) > 0.9,
}

table = pa.Table.from_pydict(cols)
pq.write_table(table, f'test_file.snappy.parquet', compression='snappy')
csv_opts = pa.csv.WriteOptions(include_header=False)
pa.csv.write_csv(table, 'test_file.csv', csv_opts)
