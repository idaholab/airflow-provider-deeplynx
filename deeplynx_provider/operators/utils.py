# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

import json
from airflow.exceptions import AirflowException
from deep_lynx.configuration import Configuration

def convert_config_from_str(config_str):
    """
    Convert deeplynx_config from str to dict if necessary.
    """
    if isinstance(config_str, str):
        return json.loads(config_str)
    return config_str

def reconstruct_config(config_dict):
    """
    Reconstruct the Configuration object from deeplynx_config.
    """
    if config_dict:
        config = Configuration()
        for key, value in config_dict.items():
            setattr(config, key, value)
        return config
    return None

def reconstruct_config_str(config_str):
    """
    Reconstruct the Configuration object from config string or dict.
    """
    config_dict = convert_config_from_str(config_str)
    config = reconstruct_config(config_dict)

    return config
