//Indexe anlegen
CREATE INDEX stationid_range_index IF NOT EXISTS FOR (n:Station) ON (n.stationId);
CREATE POINT INDEX longitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.long);
CREATE POINT INDEX latitude_point_index IF NOT EXISTS FOR (n:Station) ON (n.lat);
CREATE INDEX stationname_range_index IF NOT EXISTS FOR (n:Station) ON (n.name);
CREATE INDEX trip_index IF NOT EXISTS FOR (t:Trip) ON (t.duration,t.validFrom,t.validTo);

// Laden der Station Nodes
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`Start Station ID`)})
     ON CREATE SET
         s1.name = row.`Start Station Name`,
         s1.lat = toFloat(row.`Start Station Latitude`),
         s1.long = toFloat(row.`Start Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201701-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`End Station ID`)})
     ON CREATE SET
         s1.name = row.`End Station Name`,
         s1.lat = toFloat(row.`End Station Latitude`),
         s1.long = toFloat(row.`End Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201701-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`Trip Duration`) AS duration,
  apoc.date.parse(row.`Start Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`Stop Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
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
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//February

// Laden der Station Nodes
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201702-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`Start Station ID`)})
     ON CREATE SET
         s1.name = row.`Start Station Name`,
         s1.lat = toFloat(row.`Start Station Latitude`),
         s1.long = toFloat(row.`Start Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201702-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`End Station ID`)})
     ON CREATE SET
         s1.name = row.`End Station Name`,
         s1.lat = toFloat(row.`End Station Latitude`),
         s1.long = toFloat(row.`End Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201702-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`Trip Duration`) AS duration,
  apoc.date.parse(row.`Start Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`Stop Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
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
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//March

// Laden der Station Nodes
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201703-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`Start Station ID`)})
     ON CREATE SET
         s1.name = row.`Start Station Name`,
         s1.lat = toFloat(row.`Start Station Latitude`),
         s1.long = toFloat(row.`Start Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201703-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`End Station ID`)})
     ON CREATE SET
         s1.name = row.`End Station Name`,
         s1.lat = toFloat(row.`End Station Latitude`),
         s1.long = toFloat(row.`End Station Longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201703-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`Trip Duration`) AS duration,
  apoc.date.parse(row.`Start Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`Stop Time`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
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
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//April
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201704-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201704-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201704-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//May
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201705-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201705-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201705-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//June

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201706-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201706-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201706-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//July

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201707-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201707-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201707-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;


//August

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201708-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201708-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201708-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//September
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201709-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201709-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201709-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//October

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201710-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201710-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201710-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//November
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201711-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201711-citibike-tripdata.csv_2.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201711-citibike-tripdata.csv_2.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

//December
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`start station id`)})
     ON CREATE SET
         s1.name = row.`start station name`,
         s1.lat = toFloat(row.`start station latitude`),
         s1.long = toFloat(row.`start station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ',' RETURN row",
    "MERGE (s1:Station {stationId: toInteger(row.`end station id`)})
     ON CREATE SET
         s1.name = row.`end station name`,
         s1.lat = toFloat(row.`end station latitude`),
         s1.long = toFloat(row.`end station longitude`)",
    {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///201712-citibike-tripdata.csv_1.csv" AS row FIELDTERMINATOR "," RETURN row',
  'WITH row,
  toInteger(row.`tripduration`) AS duration,
  apoc.date.parse(row.`starttime`, "ms", "yyyy-MM-dd HH:mm:ss") AS startTime,
  apoc.date.parse(row.`stoptime`, "ms", "yyyy-MM-dd HH:mm:ss") AS stopTime
  CREATE (t:Trip {
    duration: duration,
    validFrom: datetime({epochMillis: startTime}),
    validTo: datetime({epochMillis: stopTime})
  })',
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///201712-citibike-tripdata.csv_1.csv' AS row FIELDTERMINATOR ','
   WITH row,
     toInteger(row.`start station id`) AS startId,
     toInteger(row.`end station id`) AS endId,
     toInteger(row.`tripduration`) AS duration,
     apoc.date.parse(row.`starttime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS startTime,
     apoc.date.parse(row.`stoptime`, 'ms', 'yyyy-MM-dd HH:mm:ss') AS stopTime
   WITH startId, endId, duration, startTime, stopTime
   WHERE startTime IS NOT NULL AND stopTime IS NOT NULL
   MATCH (s:Station {stationId: startId})
   MATCH (e:Station {stationId: endId})
   MATCH (t:Trip {
     duration: duration,
     validFrom: datetime({epochMillis: startTime}),
     validTo: datetime({epochMillis: stopTime})
   })
   RETURN s, e, t
   "
,
  "
   MERGE (t)-[:HAS_START]->(s)
   MERGE (t)-[:HAS_END]->(e)",
  {batchSize: 1000, parallel: false}
) YIELD batches, total
RETURN batches, total;
