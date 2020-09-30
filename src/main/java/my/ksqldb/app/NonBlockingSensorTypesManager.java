package my.ksqldb.app;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.FieldInfo;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A simple demo that showcases the Java client for ksqlDB.
 *
 * <p>Consider an IoT use case where multiple types of sensors each send readings to specific
 * Kafka topics, one for each sensor type. There may be multiple sensors of each type, all
 * reporting data to the same topic. Each sensor is identified by a unique sensor ID, and the
 * data schema for each sensor type is assumed to have a string-type {@code sensorID} value
 * column accordingly.
 *
 * <p>A {@code NonBlockingSensorTypesManager} supports registering new sensor types, querying
 * specific sensors for their latest values, and removing sensor types (and cleaning up the
 * associated resources). The methods in this class are non-blocking, and showcase
 * {@code CompletableFuture} composition. See {@link BlockingSensorTypesManager} for the
 * equivalent blocking versions, without {@code CompletableFuture} composition.
 *
 * <p>See the associated blog post at
 * https://www.confluent.io/blog/ksqldb-java-client-iot-inspired-demo/ for more.
 */
public class NonBlockingSensorTypesManager {

  private final Client client;

  /**
   * @param serverHost ksqlDB server address
   * @param serverPort ksqlDB server port
   */
  public NonBlockingSensorTypesManager(String serverHost, int serverPort) {
    ClientOptions options = ClientOptions.create()
        .setHost(serverHost)
        .setPort(serverPort);
    this.client = Client.create(options);
  }

  /**
   * Adds a new sensor type by creating a ksqlDB stream to read raw sensor data from the
   * relevant topic, and materializing a ksqlDB table to allow pull queries for retrieving
   * the latest values for particular sensors by sensor ID.
   *
   * <p>Assumes the topic data is Avro-serialized, and the relevant Avro schema has already
   * been registered in Schema Registry.
   *
   * <p>This method returns promptly and does NOT block until the ksqlDB stream and table
   * have been created.
   *
   * @param sensorType name of the new sensor type to add
   * @param sensorReadingsTopic Kafka topic name for where to find data for this sensor type
   * @return a future that completes once the ksqlDB server has acknowledged the CREATE STREAM
   *         and CREATE TABLE requests. If an error is encountered, this future is failed.
   */
  public CompletableFuture<Void> addSensorType(String sensorType, String sensorReadingsTopic) {
    // Create ksqlDB stream
    String createStreamSql = "CREATE STREAM " + sensorType
        + " WITH (KAFKA_TOPIC='" + sensorReadingsTopic + "', VALUE_FORMAT='AVRO');";
    return client.executeStatement(createStreamSql)
        // Describe the newly create stream
        .thenCompose(executeStatementResult -> client.describeSource(sensorType))
        .thenCompose(sourceDescription -> {
          List<String> columnNames = sourceDescription.fields().stream()
              .map(FieldInfo::name)
              .collect(Collectors.toList());

          // Create materialized view
          String tableName = sensorType + "_materialized";
          String materializedViewSql = "CREATE TABLE " + tableName
              + " AS SELECT sensorID, "
              + columnNames.stream()
              .filter(column -> !column.equalsIgnoreCase("sensorID"))
              .map(column -> "LATEST_BY_OFFSET(`" + column + "`) AS `" + column + "`")
              .collect(Collectors.joining(", "))
              + " FROM " + sensorType
              + " GROUP BY sensorID"
              + " EMIT CHANGES;";
          Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

          return client.executeStatement(materializedViewSql, properties);
        // Simplify return signature by returning null
        }).thenApply(executeStatementResult -> null);
  }

  /**
   * Removes an existing sensor type, by dropping the relevant ksqlDB stream and cleaning up
   * the materialized ksqlDB table that allows pull queries for retrieving the latest sensor
   * values for sensors of this type. An exception will be thrown if the ksqlDB stream, table,
   * or query that populates the table are not found.
   *
   * <p>This method returns promptly and does NOT block until the ksqlDB stream and table
   * have been dropped.
   *
   * @param sensorType name of the sensor type to remove
   * @return a future that completes once the ksqlDB server has acknowledged the DROP STREAM,
   *         DROP TABLE, and TERMINATE query requests. If an error is encountered, this future
   *         is failed.
   */
  public CompletableFuture<Void> removeSensorType(String sensorType) {

    String tableName = sensorType + "_materialized";

    return client.describeSource(sensorType)
        .thenCombine(client.describeSource(tableName),
            (streamDescription, tableDescription) -> {
              Set<String> streamReadQueries = getQueryIds(streamDescription.readQueries());
              Set<String> tableWriteQueries = getQueryIds(tableDescription.writeQueries());
              // The query that populates the materialized view reads from the stream
              // and writes to the table
              Set<String> streamReadTableWriteQueries = new HashSet<>(streamReadQueries);
              streamReadTableWriteQueries.retainAll(tableWriteQueries);
              // There should be exactly one such query
              if (streamReadTableWriteQueries.size() != 1) {
                throw new RuntimeException("Unexpected number queries populating the materialized view");
              }
              // The ID of the query that populates the materialized view
              String queryId = streamReadTableWriteQueries.stream().findFirst().orElse(null);

              return queryId;
            }).thenCompose(queryId -> client.executeStatement("TERMINATE " + queryId + ";"))
        .thenCompose(result -> client.executeStatement("DROP TABLE " + tableName + " DELETE TOPIC;"))
        .thenCompose(result -> client.executeStatement("DROP STREAM " + sensorType + ";"))
        // Simplify return signature by returning null
        .thenApply(result -> null);
  }

  /**
   * Returns the latest sensor readings for the sensor of the specified type with the specified ID.
   *
   * @param sensorType name of the sensor type
   * @param sensorID ID of the sensor to return latest values for
   * @return a future containing the latest sensor readings
   */
  public CompletableFuture<List<Row>> getLatestReadings(String sensorType, String sensorID) {
    String tableName = sensorType + "_materialized";
    String pullQuerySql = "SELECT * FROM " + tableName
        + " WHERE sensorID='" + sensorID + "';";
    return client.executeQuery(pullQuerySql);
  }

  private static Set<String> getQueryIds(List<QueryInfo> queries) {
    return queries.stream()
        .map(QueryInfo::getId)
        .collect(Collectors.toSet());
  }
}
