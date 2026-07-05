# OpenSky State Stream Engine

1. python source connector script 
    - makes api calls 
    - rate_limit - 4000 requests/day.
    - acts as a producer for the red panda cluster ??? Ned to differentiate the concerns
    - setup kafka cluster using redpanda



THoughts - 
1. kafka stream - good for my usecase since the data is small but I wanrt to know about watermarking and out of order data
2. learn apache flink
3. write to iceberg table. is there anything known as iceberg streamiong table?
5. how much will it take to do both kafka streams and flink?
6. learn pub sub too
7. learn streaming concepts

1. Use redpanda(kafka), setup topics and partitions (1 day)
2. setup kafka streams pipeline ( 4 days )
3. learn apache flink (3 days)
4. setup flink pipeline (4 days)
5. learn spark structured streaming (3 days)
6. Use spark structured streaming in any project (4 days)
7. Learn Pub/Sub and use it (2 days)
8. Learn CDC concepts
9. Learn Google DataStreams


### Phase 1: The Hands-On Core (Days 1–8)

Spin up your Redpanda cluster. Write your Scala Kafka Streams pipeline to process the incoming real-time engine data. Focus heavily on mastering watermarks, out-of-order data processing, and stateful windowing here. This proves you can build production-ready, lightweight streaming microservices.

### Phase 2: The Analytical Storage & Lakehouse Core (Days 9–14)

Take that exact same Redpanda data source, but switch your processing engine to Spark Structured Streaming (which leverages your core strengths). Write micro-batches directly into Apache Iceberg tables. Focus your learning on the downstream challenges: file compaction, snapshot isolation, and how streaming commits interact with the Iceberg metadata catalog.

### Phase 3: Architectural Mastery (Days 15–21)

Do not write Flink code. Instead, spend 2 days learning Flink's architecture conceptually. Learn exactly why someone would swap your Kafka Streams app or your Spark Streaming job out for Flink (e.g., true continuous event processing, global state management, unified batch/stream API). Read up on log-based CDC patterns and how tools like Google Datastream stream database changes into standard storage layers.

This approach allows you to walk into an interview and say: "I built our core real-time processing layer using Kafka Streams for its lightweight microservice footprint, and used Spark Structured Streaming to handle high-throughput ingestion into our Iceberg lakehouse. However, if our use case required heavy, cross-stream stateful analytics across massive time windows, I would architect the compute layer around Apache Flink instead because..."



The 5-Day Execution Blueprint

Here is a realistic, senior-level sprint plan for the week.

- Day 1: The Plumber (SerDes & Stateless)

    - Set up Redpanda via Docker.

    - Write the Scala Case Classes to match the OpenSky JSON array payload.

    - Build a purely stateless KStream: Read the topic, filter out aircraft missing coordinate data (null lat/long), map the payload into a clean JSON structure, and write it to a clean_vectors topic.

- Day 2: The Tracker (KTable & State Stores)

    - Read from the clean_vectors topic.

    - Use .groupByKey() (ensure the key is icao24).

    - Aggregate the stream into a KTable that maintains the absolute latest state vector for every aircraft currently in the sky.

    - Inspect your local /tmp/kafka-streams directory to physically watch the RocksDB state store update.

- Day 3: The Analyst (Windowing & Out-of-Order Data)

    - Branch your stream to create a windowed aggregation (e.g., a 5-minute Tumbling Window calculating average velocity).

    - Configure a Grace Period on the window.

    - The Senior Flex: Deliberately write a quick script to inject an OpenSky record with a timestamp from 10 minutes ago, and verify via logs that the watermark successfully drops it.

- Day 4: The Enricher (GlobalKTable Joins)

    - Push your static Airline CSV data into a new Redpanda topic (airline_metadata).

    - Read that topic as a GlobalKTable in your Scala app.

    - Perform a KStream-GlobalKTable join to append the full Airline Name to the live, moving flight vector before pushing it to the final output topic.

- Day 5: The Engineer (TopologyTestDriver)

    - Do not rely on spinning up Redpanda every time to test your logic.

    - Write 2-3 unit tests using Kafka's TopologyTestDriver. This proves to an interviewer that you know how to build deterministic, CI/CD-friendly streaming code without requiring a live cluster.

- day-4 join with GlobalKTable
- day 5 - stream stream join
- day 6 schema registry
- day 7 ksqldb
- day 8 topologu test 