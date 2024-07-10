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

def IntrospectionQueryResponseToFieldsList(introspection_response, type_name):
    fields_list = []
    response_data = introspection_response.to_dict()
    validate_introspection_response(response_data, type_name)
    fields_array = response_data['data']['__type']['fields']
    for fields_obj in fields_array:
        kind = fields_obj['type']['kind']
        # TODO:
        if kind == 'SCALAR':
            name = fields_obj['name']
            fields_list.append(name)

    return fields_list

def validate_introspection_response(response_data, type_name):
    from airflow.exceptions import AirflowException

    try:
        if response_data.get('data', {}).get('__type') is None:
            raise AirflowException(f"The Type named {type_name} is not present in the given DeepLynx container")
    except (ValueError, TypeError) as e:
        raise AirflowException(f"Invalid response data: {e}")

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
