# -*- coding: utf-8 -*-
# Python

"""Copyright (c) Alexander Fedotov.
This source code is licensed under the license found in the
LICENSE file in the root directory of this source tree.
"""

import os
import yaml


def go_configure() -> dict:
    # Location of the file that is running
    WHERE_ARE_WE = os.path.dirname(__file__)
    config_path = os.path.join(WHERE_ARE_WE, 'config.yaml')
    with open(config_path, 'r') as yaml_file:
        config_yaml = yaml_file.read()
    return yaml.load(config_yaml, Loader=yaml.FullLoader)
