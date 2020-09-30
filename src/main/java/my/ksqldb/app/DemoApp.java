package my.ksqldb.app;

import io.confluent.ksql.api.client.Row;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Example usage for the {@link BlockingSensorTypesManager} and
 * {@link NonBlockingSensorTypesManager} classes. See the associated blog post at
 * https://www.confluent.io/blog/ksqldb-java-client-iot-inspired-demo/ for more.
 *
 * <p>Pre-requisites for running this demo:
 * <p><ul>
 *   <li> ksqlDB server is running at the {@code KSQLDB_SERVER_HOST} and
 *        {@code KSQLDB_SERVER_HOST_PORT} specified below
 *   <li> ksqlDB server has Confluent Schema Registry enabled
 *   <li> Kafka topic {@code drone_locations} exists
 *   <li> Avro schema {@code drone_locations-value} corresponding to the SQL schema
 *        {@code (sensorID VARCHAR, latitude DOUBLE, longitude DOUBLE, altitude DOUBLE)}
 *        exists in Schema Registry
 *   <li> Kafka topic {@code drone_locations} contains the records
 *        {@code ("drone321", 37.83, 122.29, 98.2)} and {@code ("drone814", 47.61, 122.33, 18.1)}
 * </ul>
 * <p>For demo purposes, these pre-requisites may be achieved by starting the ksqlDB server,
 * with Schema Registry enabled, and issuing the following commands:
 * <p><ul>
 *   <li> CREATE STREAM temp (sensorID VARCHAR, latitude DOUBLE, longitude DOUBLE, altitude DOUBLE)
 *        WITH (KAFKA_TOPIC='drone_locations', VALUE_FORMAT='Avro', PARTITIONS=4);
 *   <li> INSERT INTO temp (sensorID, latitude, longitude, altitude) VALUES
 *        ('drone321', 37.83, 122.29, 98.2);
 *   <li> INSERT INTO temp (sensorID, latitude, longitude, altitude) VALUES
 *        ('drone814', 47.61, 122.33, 18.1);
 *   <li> DROP STREAM temp;
 * </ul>
 */
public class DemoApp {

  public static final String KSQLDB_SERVER_HOST = "localhost";
  public static final int KSQLDB_SERVER_HOST_PORT = 8088;

  public static final String DRONE_LOCATIONS_TOPIC = "drone_locations";
  public static final String DRONE_LOCATIONS_TYPE = "drone_locations";

  public static void main(final String[] args) {
    System.out.println("*** Demo-ing usage of BlockingSensorTypesManager ***");
    blockingUsage();

    System.out.println("*** Demo-ing usage of NonBlockingSensorTypesManager ***");
    nonBlockingUsage();

    System.exit(0);
  }

  private static void blockingUsage() {
    BlockingSensorTypesManager blockingSTM =
        new BlockingSensorTypesManager(KSQLDB_SERVER_HOST, KSQLDB_SERVER_HOST_PORT);

    // Add sensor type
    blockingSTM.addSensorType(DRONE_LOCATIONS_TYPE, DRONE_LOCATIONS_TOPIC);
    waitForMaterializedStateStore();

    // Query latest sensor readings
    printReadings(blockingSTM.getLatestReadings(DRONE_LOCATIONS_TYPE, "drone321"));
    printReadings(blockingSTM.getLatestReadings(DRONE_LOCATIONS_TYPE, "drone814"));

    // Remove sensor type
    blockingSTM.removeSensorType(DRONE_LOCATIONS_TYPE);
  }

  private static void nonBlockingUsage() {
    NonBlockingSensorTypesManager nonBlockingSTM =
        new NonBlockingSensorTypesManager(KSQLDB_SERVER_HOST, KSQLDB_SERVER_HOST_PORT);

    CompletableFuture<Void> cf = nonBlockingSTM
        // Add sensor type
        .addSensorType(DRONE_LOCATIONS_TYPE, DRONE_LOCATIONS_TOPIC)
        .thenRunAsync(DemoApp::waitForMaterializedStateStore)
        // Query latest sensor readings
        .thenCompose(v -> nonBlockingSTM.getLatestReadings(DRONE_LOCATIONS_TYPE, "drone321"))
        .thenApply(results -> { printReadings(results); return null; })
        .thenCompose(v -> nonBlockingSTM.getLatestReadings(DRONE_LOCATIONS_TYPE, "drone814"))
        .thenApply(results -> { printReadings(results); return null; })
        // Remove sensor type
        .thenCompose(v -> nonBlockingSTM.removeSensorType(DRONE_LOCATIONS_TYPE));

    try {
      // For demo purposes, block and wait for execution
      cf.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void waitForMaterializedStateStore() {
    try {
      // Querying for latest values immediately after adding a new sensor type may fail because the
      // state store for pull queries is not yet ready. Here we wait a bit to avoid this.
      System.out.println("Waiting for state store to become warm...");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for state store to become warm");
    }
  }

  private static void printReadings(List<Row> readings) {
    System.out.println("Received latest readings with size: " + readings.size());
    for (Row row : readings) {
      System.out.println("\t" + row.values());
    }
  }

}
