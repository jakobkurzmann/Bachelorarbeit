from sklearn.cluster import KMeans


def create_embedding(graph_datascience_object, projected_graph):
    result = graph_datascience_object.fastRP.stream(projected_graph, embeddingDimension=64)
    embedding = result["embedding"].tolist()
    return embedding, result



