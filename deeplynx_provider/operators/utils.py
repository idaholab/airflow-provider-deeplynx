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

def type_mapping_from_dict(dict_obj):
    """Creates a TypeMapping object from a dictionary."""
    from deep_lynx.models.type_mapping import TypeMapping
    return TypeMapping(
        active=dict_obj.get('active'),
        id=dict_obj.get('id'),
        container_id=dict_obj.get('container_id'),
        data_source_id=dict_obj.get('data_source_id'),
        shape_hash=dict_obj.get('shape_hash'),
        created_at=dict_obj.get('created_at'),
        modified_at=dict_obj.get('modified_at'),
        sample_payload=dict_obj.get('sample_payload'),
        created_by=dict_obj.get('created_by'),
        modified_by=dict_obj.get('modified_by'),
        transformations=dict_obj.get('transformations'),
        resulting_metatype_name=dict_obj.get('resulting_metatype_name'),
        resulting_metatype_relationship_name=dict_obj.get('resulting_metatype_relationship_name')
    )

def extract_first_matching_nested_object(json_objects, nested_key, match_keys):
    """
    Extracts the first JSON object that contains a nested object matching all specified keys from a list of JSON objects.

    Args:
    - json_objects (list of dict): A list of JSON objects (dictionaries) to search through.
    - nested_key (str): The key used to access the nested object in each JSON object.
    - match_keys (list of str): A list of keys that must be present in the nested object.

    Returns:
    - dict or None: The first JSON object containing a nested object that matches all specified keys, or None if no match is found.
    """
    for obj in json_objects:
        if nested_key in obj:
            nested_obj = obj[nested_key]
            if all(prop in nested_obj for prop in match_keys):
                return obj
    return None
