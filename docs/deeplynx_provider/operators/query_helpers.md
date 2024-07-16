Module deeplynx_provider.operators.query_helpers
================================================





Functions
---------


`IntrospectionQueryResponseToFieldsList(introspection_response, type_name)`
:   




`validate_introspection_response(response_data, type_name)`
:   



Classes
-------

`GraphQLIntrospectionQuery(type_name)`
:   

    ### Methods

    `generate_query(self)`
    :




`GraphQuery(root_node, depth)`
:   

    ### Methods

    `generate_query(self)`
    :




`MetatypeQuery(metatype_name, properties, limit=1000)`
:   

    ### Methods

    `generate_query(self)`
    :




`QueryType(*args, **kwds)`
:   Create a collection of name/value pairs.
    
    Example enumeration:
    
    >>> class Color(Enum):
    ...     RED = 1
    ...     BLUE = 2
    ...     GREEN = 3
    
    Access them by:
    
    - attribute access::
    
    >>> Color.RED
    <Color.RED: 1>
    
    - value lookup:
    
    >>> Color(1)
    <Color.RED: 1>
    
    - name lookup:
    
    >>> Color['RED']
    <Color.RED: 1>
    
    Enumerations can be iterated over, and know how many members they have:
    
    >>> len(Color)
    3
    
    >>> list(Color)
    [<Color.RED: 1>, <Color.BLUE: 2>, <Color.GREEN: 3>]
    
    Methods can be added to enumerations, and members can have their own
    attributes -- see the documentation for details.

    ### Ancestors (in MRO)

    * enum.Enum

    ### Class variables

    `GRAPH`
    :

    `METATYPE`
    :

    `RELATIONSHIP`
    :

    `TIMESERIES`
    :




`RelationshipQuery(relationship_name, limit=1000)`
:   

    ### Methods

    `generate_query(self)`
    :




`TimeSeriesQuery(properties, limit=1000, sort_by='timestamp', sort_desc=False)`
:   

    ### Methods

    `generate_query(self)`
    :