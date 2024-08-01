from graphdatascience import GraphDataScience


# Create a connection to the database. Returns a GraphDataScience object.
def create_database_connection():
    gds = GraphDataScience("neo4j://localhost:7687", auth=("neo4j", "#Bachelorarbeit"))
    gds.set_database("neo4j")
    return gds
