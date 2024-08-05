from database_connection import create_database_connection
from project_graph import project_graph, getGraph
from simple_embeddings import create_embedding
from clustering import kMeans


def main():
    try:
        gds = create_database_connection()
        graph_name = "city_bikes"
        node_labels = ["Station", "Trip", "Bike", "User"]
        relationship_labels = ["HAS_START", "HAS_END", "PARTICIPATED_IN", "USED_IN"]
        project_graph(gds, graph_name, node_labels, relationship_labels)
        G = getGraph(gds, graph_name)
        embedding, df = create_embedding(gds, G)
        kMeans(embedding, 5, 42, df)
        gds.close()
    except:
        print("Something went wrong with the connection to the database")


if __name__ == '__main__':
    main()
