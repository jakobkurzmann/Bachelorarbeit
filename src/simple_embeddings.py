from graphdatascience import GraphDataScience
from sklearn.cluster import KMeans

# Create a connection to the database
gds = GraphDataScience("neo4j://localhost:7687", auth=("neo4j", "#Bachelorarbeit"))
gds.set_database("neo4j")


# Project the graph
def project_graph():
    projected_graph = gds.graph.project(
        "city_bikes",
        ["Station", "Trip", "Bike", "User"],
        ["HAS_START", "HAS_END", "PARTICIPATED_IN", "USED_IN"],
        readConcurrency=4
    )
    return projected_graph


# Project the graph with properties
def project_graph_with_properties():
    projected_graph = gds.graph.project(
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


G = gds.graph.get("city_bikes")

embedding = gds.fastRP.stream(G, embeddingDimension=64)

print(embedding)

#TODO: Implement KMeans Clustering
kmeans = KMeans(n_clusters=4, random_state=0).fit(embedding)
print(kmeans)

#close the connection
gds.close()
