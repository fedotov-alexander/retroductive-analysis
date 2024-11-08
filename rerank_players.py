# -*- coding: utf-8 -*-
# Python

"""Copyright (c) Alexander Fedotov.
This source code is licensed under the license found in the
LICENSE file in the root directory of this source tree.
"""
from configuration import go_configure
import polars as pl

config = go_configure()

data_parquet_path = config['data_directory'] + config['players_directory']
df = pl.read_parquet(data_parquet_path + 'long_players.parquet').sort('lifespan', descending=True)

df = df.with_row_count("number", offset=1)

conversion_df = df.select(["number","afid","uid","client_id","first_seen"])
print(conversion_df.glimpse(return_as_string=True))
anonymised_df = df.drop(["uid","afid","client_id"])
print(anonymised_df.glimpse(return_as_string=True))

print(df.glimpse(return_as_string=True))
...