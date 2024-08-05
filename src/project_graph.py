def project_graph(graph_datascience_object,graph_name,node_labels,relationshsip_labels):
    graph_datascience_object.graph.project(
        graph_name,
        node_labels,
        relationshsip_labels,
        readConcurrency=4
    )

def project_graph_with_properties(graph_datascience_object):
    projected_graph = graph_datascience_object.graph.project(
        "city_bikes",
        {
            "Station": {
                "properties": ["stationId"]
            },
            "Trip": {
            },
            "Bike": {
            },
            "User":
                {
                }
        },
        ["HAS_START", "HAS_END", "PARTICIPATED_IN", "USED_IN"],
        readConcurrency=4
    )
    return projected_graph

def getGraph(graph_datascience_object,name):
    return graph_datascience_object.graph.get(name)