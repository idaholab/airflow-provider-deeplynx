Module deeplynx_provider.operators.utils
========================================





Functions
---------


`convert_config_from_str(config_str)`
:   Convert deeplynx_config from str to dict if necessary.




`extract_first_matching_nested_object(json_objects, nested_key, match_keys)`
:   Extracts the first JSON object that contains a nested object matching all specified keys from a list of JSON objects.
    
    Args:
    - json_objects (list of dict): A list of JSON objects (dictionaries) to search through.
    - nested_key (str): The key used to access the nested object in each JSON object.
    - match_keys (list of str): A list of keys that must be present in the nested object.
    
    Returns:
    - dict or None: The first JSON object containing a nested object that matches all specified keys, or None if no match is found.




`reconstruct_config(config_dict)`
:   Reconstruct the Configuration object from deeplynx_config.




`reconstruct_config_str(config_str)`
:   Reconstruct the Configuration object from config string or dict.




`type_mapping_from_dict(dict_obj)`
:   Creates a TypeMapping object from a dictionary.