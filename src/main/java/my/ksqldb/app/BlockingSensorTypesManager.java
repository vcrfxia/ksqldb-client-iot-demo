package my.ksqldb.app;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.FieldInfo;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.SourceDescription;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
 * <p>A {@code BlockingSensorTypesManager} supports registering new sensor types, querying
 * specific sensors for their latest values, and removing sensor types (and cleaning up the
 * associated resources). The methods in this class are blocking. See
 * {@link NonBlockingSensorTypesManager} for non-blocking versions.
 *
 * <p>See the associated blog post at
 * https://www.confluent.io/blog/ksqldb-java-client-iot-inspired-demo/ for more.
 */
public class BlockingSensorTypesManager {

  private final Client client;

  /**
   * @param serverHost ksqlDB server address
   * @param serverPort ksqlDB server port
   */
  public BlockingSensorTypesManager(String serverHost, int serverPort) {
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
   * <p>This method blocks until the ksqlDB server has acknowledged the CREATE STREAM and
   * CREATE TABLE requests.
   *
   * @param sensorType name of the new sensor type to add
   * @param sensorReadingsTopic Kafka topic name for where to find data for this sensor type
   */
  public void addSensorType(String sensorType, String sensorReadingsTopic) {
    // Create ksqlDB stream
    String createStreamSql = "CREATE STREAM " + sensorType
        + " WITH (KAFKA_TOPIC='" + sensorReadingsTopic + "', VALUE_FORMAT='AVRO');";
    try {
      client.executeStatement(createStreamSql).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to create stream", e);
    }

    // Fetch schema
    List<String> columnNames;
    try {
      SourceDescription description = client.describeSource(sensorType).get();
      columnNames = description.fields().stream()
          .map(FieldInfo::name)
          .collect(Collectors.toList());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to describe stream", e);
    }

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
    try {
      client.executeStatement(materializedViewSql, properties).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to create materialized view", e);
    }
  }

  /**
   * Removes an existing sensor type, by dropping the relevant ksqlDB stream and cleaning up
   * the materialized ksqlDB table that allows pull queries for retrieving the latest sensor
   * values for sensors of this type. An exception will be thrown if the ksqlDB stream, table,
   * or query that populates the table are not found.
   *
   * <p>This method blocks until the ksqlDB server has acknowledged the DROP STREAM, DROP TABLE,
   * and TERMINATE query requests.
   *
   * @param sensorType name of the sensor type to remove
   */
  public void removeSensorType(String sensorType) {

    String tableName = sensorType + "_materialized";

    // Find query ID
    SourceDescription streamDescription, tableDescription;
    try {
      streamDescription = client.describeSource(sensorType).get();
      tableDescription = client.describeSource(tableName).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to describe stream/table", e);
    }
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

    // Terminate query, drop stream and table
    try {
      client.executeStatement("TERMINATE " + queryId + ";").get();
      client.executeStatement("DROP TABLE " + tableName + " DELETE TOPIC;").get();
      client.executeStatement("DROP STREAM " + sensorType + ";").get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to terminate the query and drop the stream and table", e);
    }
  }

  /**
   * Returns the latest sensor readings for the sensor of the specified type with the specified ID.
   *
   * @param sensorType name of the sensor type
   * @param sensorID ID of the sensor to return latest values for
   * @return latest sensor readings
   */
  public List<Row> getLatestReadings(String sensorType, String sensorID) {
    String tableName = sensorType + "_materialized";
    String pullQuerySql = "SELECT * FROM " + tableName
        + " WHERE sensorID='" + sensorID + "';";
    try {
      return client.executeQuery(pullQuerySql).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to get latest readings", e);
    }
  }

  private static Set<String> getQueryIds(List<QueryInfo> queries) {
    return queries.stream()
        .map(QueryInfo::getId)
        .collect(Collectors.toSet());
  }
}
