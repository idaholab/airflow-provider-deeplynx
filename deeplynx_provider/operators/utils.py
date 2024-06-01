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

def check_host_mismatch(host, config_str, logger):
    """
    Check for host mismatch and log a warning if there is a mismatch.
    """
    config_dict = convert_config_from_str(config_str)
    if host and config_dict and host != config_dict.get('host'):
        logger.warning(f"The provided host '{host}' does not match the host in deeplynx_config '{config_dict.get('host')}'.")

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
    config_dict = convert_config_from_str(config_str)
    config = reconstruct_config(config_dict)

    return config
