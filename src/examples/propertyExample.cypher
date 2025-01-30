
//Erstellen des Vektor Indexes
CREATE VECTOR INDEX temporalEmbeddingIndex IF NOT EXISTS
FOR (t:Trip)
ON t.temporalEmbeddingWithProperties
OPTIONS {indexConfig: {
    `vector.dimensions`: 64,
    `vector.similarity_function`: 'cosine'
}}

//Funktion um die Anzahl der jeweiligen Trips zwischen 2 Stationen zu ermitteln
MATCH (e:Station)<-[:HAS_END]-(t:Trip)-[:HAS_START]->(s:Station)
WITH s.name AS startStation, e.name AS endStation, COUNT(t) AS totalTrips
RETURN startStation, endStation, totalTrips
ORDER BY totalTrips DESC;

//Ausgeben eines Trips mit Start- und Endstation
MATCH (e:Station)<-[:HAS_END]-(t:Trip{validFrom: datetime("2017-01-16T11:39:51Z"), validTo:datetime("2017-01-16T11:47:34Z")})-[:HAS_START]->(s:Station)
RETURN e,s,t

//Abfrage nach ähnlichsten Embeddings
MATCH (t:Trip{validFrom: datetime("2017-01-16T11:39:51Z"), validTo:datetime("2017-01-16T11:47:34Z")})
CALL db.index.vector.queryNodes('temporalEmbeddingIndex',20,t.temporalEmbedding)
YIELD node AS trip, score
MATCH (e:Station)<-[:HAS_END]-(trip)-[:HAS_START]->(s:Station)
RETURN e,trip,s

//Wie viele Trips gab es im Januar von der Station ... bis zu der Station ...
MATCH (e:Station)<-[:HAS_END]-(:Trip{validFrom: datetime("2017-01-16T11:39:51Z"), validTo:datetime("2017-01-16T11:47:34Z")})-[:HAS_START]->(s:Station)
MATCH (e)<-[:HAS_END]-(t:Trip)-[:HAS_START]->(s)
RETURN COUNT(t)