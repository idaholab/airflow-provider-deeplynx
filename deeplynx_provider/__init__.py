# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

__version__ = "1.0.0"

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-deeplynx",  # Required
        "name": "DeepLynx",  # Required
        "description": "Apache Airflow provider for DeepLynx.",  # Required
        "connection-types": [
            {
                "connection-type": "deeplynx",
                "hook-class-name": "deeplynx_provider.hooks.deeplynx.DeepLynxHook"
            }
        ],
        "versions": [__version__],  # Required
    }
