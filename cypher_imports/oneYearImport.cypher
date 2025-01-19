//Indexe anlegen
CREATE INDEX stationid_range_index IF NOT EXISTS FOR (n:Station) ON (n.stationId);
CREATE POINT INDEX longitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.long);
CREATE POINT INDEX latitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.lat);
CREATE INDEX stationname_range_index IF NOT EXISTS FOR (n:Station) ON (n.name);
CREATE INDEX trip_index IF NOT EXISTS FOR (t:Trip) ON (t.duration,t.validFrom,t.validTo);
// Laden der Station Nodes
LOAD CSV WITH HEADERS FROM'file:///201701-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`Start Station ID`),name:row.`Start Station Name`,lat:toFloat(row.`Start Station Latitude`),long:toFloat(row.`Start Station Longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201701-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`End Station ID`),name:row.`End Station Name`,lat:toFloat(row.`End Station Latitude`),long:toFloat(row.`End Station Longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201701-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
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
  "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     toInteger(row.`Trip Duration`) AS duration,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
) YIELD batches, total
RETURN batches, total;

//Februar

LOAD CSV WITH HEADERS FROM'file:///201702-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`Start Station ID`),name:row.`Start Station Name`,lat:toFloat(row.`Start Station Latitude`),long:toFloat(row.`Start Station Longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201702-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`End Station ID`),name:row.`End Station Name`,lat:toFloat(row.`End Station Latitude`),long:toFloat(row.`End Station Longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201702-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
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
  "LOAD CSV WITH HEADERS FROM 'file:///201702-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     toInteger(row.`Trip Duration`) AS duration,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//März
LOAD CSV WITH HEADERS FROM'file:///201703-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`Start Station ID`),name:row.`Start Station Name`,lat:toFloat(row.`Start Station Latitude`),long:toFloat(row.`Start Station Longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201703-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`End Station ID`),name:row.`End Station Name`,lat:toFloat(row.`End Station Latitude`),long:toFloat(row.`End Station Longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201703-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
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
  "LOAD CSV WITH HEADERS FROM 'file:///201703-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`Start Station ID`) AS startId,
     toInteger(row.`End Station ID`) AS endId,
     toInteger(row.`Trip Duration`) AS duration,
     apoc.date.parse(row.`Start Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`Stop Time`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;


//April
LOAD CSV WITH HEADERS FROM'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201704-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201704-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//Mai

LOAD CSV WITH HEADERS FROM'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201705-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201705-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//Juni
LOAD CSV WITH HEADERS FROM'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201706-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201706-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//Juli

LOAD CSV WITH HEADERS FROM'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201707-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201707-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//August
LOAD CSV WITH HEADERS FROM'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201708-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201708-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//September
LOAD CSV WITH HEADERS FROM'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201709-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201709-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//Oktober
LOAD CSV WITH HEADERS FROM'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201710-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201710-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

//November
LOAD CSV WITH HEADERS FROM'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201711-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;

LOAD CSV WITH HEADERS FROM'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201711-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;
//Dezember
LOAD CSV WITH HEADERS FROM'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`start station id`),name:row.`start station name`,lat:toFloat(row.`start station latitude`),long:toFloat(row.`start station longitude`)})
RETURN row;
LOAD CSV WITH HEADERS FROM'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
WITH row
MERGE(s1:Station{stationId:toInteger(row.`end station id`),name:row.`end station name`,lat:toFloat(row.`end station latitude`),long:toFloat(row.`end station longitude`)})
RETURN row;
//Laden der Trip Nodes
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201712-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
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
  "LOAD CSV WITH HEADERS FROM 'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
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
)YIELD batches, total
RETURN batches, total;
