# -*- coding: utf-8 -*-
# Python

"""Copyright (c) Alexander Fedotov.
This source code is licensed under the license found in the
LICENSE file in the root directory of this source tree.
"""
from configuration import go_configure
import polars as pl


def get_players(list_of_players=None):
    """
    De-anonymize the players by adding the ids to the players dataframe.
    :return: players dataframe
    """
    config = go_configure()

    # Read the components of the path from config.
    data_parquet_path = config['data_directory'] + config['players_directory']
    ids_file_name = data_parquet_path + 'ids.parquet'
    players_file_name = data_parquet_path + 'players.parquet'

    # Read the ids dataframe.
    ids = pl.read_parquet(ids_file_name, use_pyarrow=True)
    print(ids.glimpse(return_as_string=True))

    # Drop the first_seen column from the ids dataframe
    # otherwise there will be 'first_seen' and 'first_seen_right'
    ids = ids.drop(['first_seen'])

    # Read the players dataframe.
    players = pl.read_parquet(players_file_name, use_pyarrow=True)

    # Join the ids dataframe with the players dataframe
    # on the 'number' column (with 1:1 match validation).
    combined = ids.join(players, on='number', how='inner', validate='1:1')
    if len(list_of_players) > 0:
        combined = combined.filter(pl.col('number').is_in(list_of_players))

    return combined


if __name__ == '__main__':
    numbers_of_players = [1, 2, 3, 4, 5]
    # Get the players
    players = get_players(list_of_players=numbers_of_players)
    print(players.glimpse(return_as_string=True))
    ...
