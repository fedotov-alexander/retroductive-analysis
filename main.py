# -*- coding: utf-8 -*-
# Python

"""Copyright (c) Alexander Fedotov.
This source code is licensed under the license found in the
LICENSE file in the root directory of this source tree.
"""
from configuration import go_configure


def configure():
    return go_configure()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # conf = configure()
    conf = go_configure()
    print(conf)

