# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from enum import Enum

class QueryType(Enum):
    GRAPH = "graph"
    METATYPE = "metatype"
    RELATIONSHIP = "relationship"
    TIMESERIES = "timeseries"


class GraphQLIntrospectionQuery:
    def __init__(self, type_name):
        self.type_name = type_name

    def generate_query(self):
        return f"""
{{
    __type(name: "{self.type_name}"){{
        fields{{
            name
            type{{
                name
                kind
            }}
        }}
    }}
}}
"""

# returns list of class (aka metatype) properties available to query; returns an empty list if that class is not in the DeepLynx container's ontology
def IntrospectionQueryResponseToFieldsList(introspection_response, type_name):
    import logging

    response_data = introspection_response.to_dict()
    # print(response_data)
    fields_list = []
    if response_data.get('data', {}).get('__type') is None:
        logging.warning(f"The Type named {type_name} is not present in the given DeepLynx container")
    else:
        fields_array = response_data['data']['__type']['fields']
        for fields_obj in fields_array:
            kind = fields_obj['type']['kind']
            # TODO:
            if kind == 'SCALAR':
                name = fields_obj['name']
                fields_list.append(name)

    return fields_list

class TimeSeriesQuery:
    def __init__(self, properties, limit=1000, sort_by="timestamp", sort_desc=False):
        self.properties = properties
        self.limit = limit
        self.sort_by = sort_by
        self.sort_desc = sort_desc

    def generate_query(self):
        properties_str = "\n                ".join(self.properties)
        query_template = f"""
        {{
            Timeseries (_record: {{
                limit: {self.limit},
                sortBy: "{self.sort_by}",
                sortDesc: {str(self.sort_desc).lower()}
            }}) {{
                {properties_str}
            }}
        }}
        """
        return query_template

class MetatypeQuery:
    def __init__(self, metatype_name, properties, limit=1000):
        self.metatype_name = metatype_name
        self.properties = properties
        self.limit = limit

    def generate_query(self):
        properties_str = "\n                ".join(self.properties)
        query_template = f"""
        {{
            metatypes{{
                {self.metatype_name} (
                    _record: {{
                        limit: {self.limit}
                    }}
                ) {{
                    _record{{
                        data_source_id
                        metatype_id
                        metatype_name
                    }},
                    {properties_str}
                }}
            }}
        }}
        """
        return query_template

class RelationshipQuery:
    def __init__(self, relationship_name, limit=1000):
        self.relationship_name = relationship_name
        self.limit = limit

    def generate_query(self):
        query_template = f"""
        {{
            relationships{{
                {self.relationship_name} (
                    _record: {{
                        limit: {self.limit}
                    }}
                ) {{
                    _record{{
                        id
                        relationship_name
                        origin_id
                        destination_id
                    }},
                    metadata_properties
                }}
            }}
        }}
        """
        return query_template

class GraphQuery:
    def __init__(self, root_node, depth):
        self.root_node = root_node
        self.depth = depth

    def generate_query(self):
        query_template = f"""
        {{
            graph(
                root_node: "{self.root_node}"
                depth: "{self.depth}"
            ){{
                origin_id
                origin_properties
                origin_metatype_name
                destination_id
                destination_properties
                destination_metatype_name
                depth
                edge_direction
            }}
        }}
        """
        return query_template


def get_data_from_response(json_dict: dict, query_type: QueryType):
    """
    Extract data from the query response based on the query type.

    Args:
        json_dict (dict): The JSON response dictionary from the DeepLynx API.
        query_type (QueryType): The DeepLynx QueryType.

    Returns:
        tuple: A tuple containing the data name and the extracted data.
    """
    # print(f"json_dict: {json_dict}")
    if "data" not in json_dict or json_dict.get("data") is None:
        return ("no_data", [])
    elif query_type == QueryType.GRAPH:
        graph = json_dict["data"]["graph"]
        return ("graph", graph)
    elif query_type == QueryType.METATYPE:
        metatypes = json_dict["data"]["metatypes"]
        metatype_name = next(iter(metatypes))
        metatype_values = metatypes[metatype_name]
        return (metatype_name, metatype_values)
    elif query_type == QueryType.RELATIONSHIP:
        relationships = json_dict["data"]["relationships"]
        relationship_name = next(iter(relationships))
        relationship_values = relationships[relationship_name]
        return (relationship_name, relationship_values)
    else:
        raise ValueError("Invalid query type")
