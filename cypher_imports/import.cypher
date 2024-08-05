CREATE INDEX stationid_range_index IF NOT EXISTS FOR (n:Station) ON (n.stationId);
CREATE POINT INDEX longitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.long);
CREATE POINT INDEX latitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.lat);
CREATE INDEX stationname_range_index IF NOT EXISTS FOR (n:Station) ON (n.name);
CREATE INDEX trip_index IF NOT EXISTS FOR (t:Trip) ON (t.duration,t.validFrom,t.validTo);


// Laden der Stationen
LOAD CSV WITH HEADERS FROM'file:///citibike_data.csv' AS row FIELDTERMINATOR ';'
WITH row
MERGE(s1:Station{stationId:toInteger(row.Start_Station_ID),name:row.Start_Station_Name,lat:toFloat(row.Start_Station_Latitude),long:toFloat(row.Start_Station_Longitude)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///citibike_data.csv' AS row FIELDTERMINATOR ';'
WITH row
MERGE(s1:Station{stationId:toInteger(row.End_Station_ID),name:row.End_Station_Name,lat:toFloat(row.End_Station_Latitude),long:toFloat(row.End_Station_Longitude)})
RETURN row;

//Laden der Trips
CALL apoc.periodic.iterate(
'LOAD CSV WITH HEADERS FROM "file:///citibike_data.csv" AS row FIELDTERMINATOR ";" RETURN row',
'WITH row,
toInteger(row.Trip_Duration) AS duration,
apoc.date.parse(row.Start_Time, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
apoc.date.parse(row.Stop_Time, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
MERGE (t:Trip {
duration: duration,
validFrom: datetime({epochMillis: startTime}),
validTo: datetime({epochMillis: stopTime})
})',
{batchSize: 1000, parallel: true}
) YIELD batches, total
RETURN batches, total;

//die worked
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///citibike_data.csv' AS row FIELDTERMINATOR ';'
   WITH row,
     toInteger(row.Start_Station_ID) AS startId,
     toInteger(row.End_Station_ID) AS endId,
     toInteger(row.Trip_Duration) AS duration,
     apoc.date.parse(row.Start_Time, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.Stop_Time, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   RETURN startId, endId, duration, startTime, stopTime",
  "MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
)



//QUery
LOAD CSV WITH HEADERS FROM "file:///citibike_data.csv" AS row
FIELDTERMINATOR ";"
WITH row,
  toInteger(row.Start_Station_ID) AS startId,
  toInteger(row.Start_Station_ID) AS endId,
  toInteger(row.Trip_Duration) AS duration,
  apoc.date.parse(row.Start_Time, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.Stop_Time, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
WITH duration, startTime, stopTime, startId, endId
MATCH (t:Trip {
  duration: duration,
  validFrom: datetime({epochMillis: startTime}),
  validTo: datetime({epochMillis: stopTime})
})
MATCH(s:Station{stationId:startId})
MATCH(e:Station{stationId:endId})
RETURN t,s,e
LIMIT 20;


