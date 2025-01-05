//Indexe anlegen
CREATE INDEX stationid_range_index IF NOT EXISTS FOR (n:Station) ON (n.stationId);
CREATE POINT INDEX longitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.long);
CREATE POINT INDEX latitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.lat);
CREATE INDEX stationname_range_index IF NOT EXISTS FOR (n:Station) ON (n.name);
CREATE INDEX trip_index IF NOT EXISTS FOR (t:Trip) ON (t.duration,t.validFrom,t.validTo);


// Laden der Station Nodes
LOAD CSV WITH HEADERS FROM'file:///201701-citibike-tripdata.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`Start Station ID`),name:row.`Start Station Name`,lat:toFloat(row.`Start Station Latitude`),long:toFloat(row.`Start Station Longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201701-citibike-tripdata.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`End Station ID`),name:row.`End Station Name`,lat:toFloat(row.`End Station Latitude`),long:toFloat(row.`End Station Longitude`)})
RETURN row;


//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201701-citibike-tripdata.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`Trip Duration`) AS duration,
  apoc.date.parse(row.`Start Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`Stop Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  MERGE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: true}
) YIELD batches, total
RETURN batches, total;

//Laden der Bike Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201701-citibike-tripdata.csv" AS row FIELDTERMINATOR "," RETURN DISTINCT row.`Bike ID` AS bikeId',
  'WITH toInteger(bikeId) AS bikeId
   MERGE (b:Bike {bikeId: bikeId})',
  {batchSize: 1000, parallel: true}
) YIELD batches, total
RETURN batches, total;


CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201701-citibike-tripdata.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`Trip Duration`) AS duration,
  apoc.date.parse(row.`Start Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`Stop Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  MERGE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: true}
) YIELD batches, total
RETURN batches, total;




//Laden der Beziehung zwischen Trip und Station
//HAS_START und HAS_END
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     toInteger(row.`Bike ID`) AS bikeId,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   RETURN startId, endId, startTime, stopTime, bikeId",
  "MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   MATCH (b:Bike {bikeId: bikeId})
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (e)<-[:HAS_END]-(t)-[:HAS_START]->(s)
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   CREATE (b)-[:USED_IN]->(t)
   ",
  {batchSize: 1000, parallel: false}
)

//Laden der USED_IN Beziehung
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     toInteger(row.`Bike ID`) AS bikeId,
     toInteger(row.`Trip Duration`) AS duration,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   RETURN startId, endId, duration, startTime, stopTime, bikeId",
  "MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   MATCH (b:Bike {bikeId: bikeId})
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)
   MERGE (b)-[:USED_IN]->(t)
   ",

  {batchSize: 1000, parallel: false}
)


//Laden der User Nodes Langsam!!!
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime,
     row.`User Type` AS userType,
     row.`Birth Year` AS birthYear,
     toInteger(row.`Gender`) AS gender
   RETURN startId, endId, startTime, stopTime, userType, birthYear, gender",
  "MATCH (t:Trip {
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (e)<-[:HAS_END]-(t)-[:HAS_START]->(s)
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   CREATE (u:User {
     userType: userType,
     birthYear: birthYear,
     gender: gender})
   MERGE (u)-[:PARTICIPATED_IN]->(t)
   ",
  {batchSize: 1000, parallel: true}
)






