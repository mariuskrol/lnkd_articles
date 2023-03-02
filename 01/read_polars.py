import os

import polars as pl


dir = os.getcwd()

data = pl.read_delta(dir + os.sep + "people")

data = data.groupby("state").agg(pl.count()).sort("state").limit(20)

print(data)
