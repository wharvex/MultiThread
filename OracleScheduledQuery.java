import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OracleScheduledQuery {

    private static final Logger LOGGER = Logger.getLogger(OracleScheduledQuery.class.getName());

    private static final String DB_URL      = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1";
    private static final String DB_USER     = "globaltrade";
    private static final String DB_PASSWORD = "pw";
    private static final String QUERY       = "SELECT 'hi' FROM DUAL";

    private final ScheduledExecutorService scheduler;

    public OracleScheduledQuery(int threadPoolSize) {
        this.scheduler = Executors.newScheduledThreadPool(threadPoolSize);
    }

    public ScheduledFuture<?> start(long initialDelaySeconds, long periodSeconds) {
        return scheduler.scheduleAtFixedRate(
                this::executeQuery,
                initialDelaySeconds,
                periodSeconds,
                TimeUnit.SECONDS
        );
    }

    private void executeQuery() {
        LOGGER.info("Running scheduled Oracle query...");
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(QUERY);
             ResultSet rs = stmt.executeQuery()) {

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                // Process each row — adapt column access to your schema
                LOGGER.info("Row " + rowCount + ": " + rs.getString(1));
            }
            LOGGER.info("Query complete. Rows returned: " + rowCount);

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Query failed", e);
        }
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // Load Oracle JDBC driver explicitly (required for Java 8 with older ojdbc jars)
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Oracle JDBC driver not found on classpath", e);
            return;
        }

        OracleScheduledQuery job = new OracleScheduledQuery(2);
        job.start(0, 60); // run immediately, then every 60 seconds

        // Run for 5 minutes then shut down gracefully
        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
        job.stop();
    }
}
