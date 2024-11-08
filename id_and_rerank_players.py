# -*- coding: utf-8 -*-
# Python

'''Copyright (c) Alexander Fedotov.
This source code is licensed under the license found in the
LICENSE file in the root directory of this source tree.
'''
from configuration import go_configure
import polars as pl
from polars.testing import assert_frame_equal


def rerank_and_anonymize_players():
    """
    This function reads the parquet file with players who played for more than 1 day
    and sorts them by lifespan in descending order.
    It then separates into a dataframe all the id columns and this number.
    It then anonymises the players by creating a new dataframe with removed ids.
    It then writes ans verifies these dataframes.
    :return: None
    """
    config = go_configure()

    # Read the components of the path from config.
    data_parquet_path = config['data_directory'] + config['players_directory']

    # Read the parquet file with players who played for more than 1 day
    # and sort them by lifespan in descending order.
    df = pl.read_parquet(data_parquet_path + 'long_players.parquet', use_pyarrow=True).sort('lifespan', descending=True)

    # Number the players.This number will be used as their id
    # in the rest of the analysis (for anonymisation of the players).
    df = df.with_row_index('number', offset=1)

    # Select the columns with this number, all the ids and the lifespan
    # of the players (for ranking and segmentation on the id level).
    id_conversion_df = df.select(['number', 'afid', 'uid', 'client_id', 'first_seen'])
    # print(conversion_df.glimpse(return_as_string=True))

    # Write the ids dataframe to a parquet file
    ids_file_name = data_parquet_path + 'ids.parquet'
    id_conversion_df.write_parquet(ids_file_name, use_pyarrow=True)

    # Anonymise the players by removing their ids from the dataframe.
    anonymized_players_df = df.drop(['uid','afid','client_id'])
    # print(anonymized_df.glimpse(return_as_string=True))

    # Write the players dataframe to a parquet file
    players_file_name = data_parquet_path + 'players.parquet'
    anonymized_players_df.write_parquet(players_file_name, use_pyarrow=True)

    # Make sure the files are readable and can be joined into
    # the same dataframe.
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

    # Reorder the columns of the combined dataframe as they were
    # in the original dataframe, otherwise the comparison will fail.
    order = ['number', 'afid', 'country_code', 'client_id',
             'first_seen', 'first_date', 'last_seen',
             'last_date', 'lifespan', 'uid']
    combined = combined.select(order)
    print(combined.glimpse(return_as_string=True))

    # Compare the initial dataframe with the combined dataframe
    # from two read back files.
    try:
        assert_frame_equal(df, combined)
    except AssertionError as e:
        print(e)
    # that's pretty much it.
    return


if __name__ == '__main__':
    rerank_and_anonymize_players()
    print('Done!')