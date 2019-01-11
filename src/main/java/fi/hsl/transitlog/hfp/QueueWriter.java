package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class QueueWriter {
    private static final Logger log = LoggerFactory.getLogger(QueueWriter.class);

    Connection connection;

    private QueueWriter(Connection conn) {
        connection = conn;
    }

    public static QueueWriter newInstance(Config config) throws Exception {
        final String connectionString = config.getString("db.connectionString");
        final String user = config.getString("db.username");
        final String password = config.getString("db.password");

        log.info("Connecting to the database with connection string " + connectionString);
        Connection conn = DriverManager.getConnection(connectionString, user, password);
        conn.setAutoCommit(false); // we're doing batch inserts so no auto commit
        log.info("Connection success");
        return new QueueWriter(conn);
    }

    private String createInsertStatement() {
        return new StringBuffer()
                .append("INSERT INTO VEHICLES (")
                .append("received_at, topic_prefix, topic_version,")
                .append("journey_type, is_ongoing, mode,")
                .append("owner_operator_id, vehicle_number, unique_vehicle_id,")
                //TODO more fields
                .append("veh, tst, tsi")
                .append(") VALUES (?, ?, ?, ?::JOURNEY_TYPE, ?, ?::TRANSPORT_MODE, ?, ?, ?, ?, ?, ?);")
                .toString();
    }

    public void write(List<HfpMessage> messages) throws Exception {
        //TODO max batch size
        log.info("Writing {} rows to database", messages.size());
        //TODO test using copy: https://www.postgresql.org/docs/current/sql-copy.html
        //https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html

        long startTime = System.currentTimeMillis();
        String queryString = createInsertStatement();
        try (PreparedStatement statement = connection.prepareStatement(queryString)) {

            for (HfpMessage message: messages) {
                //DEBUG hardcode stuff we don't yet parse
                statement.setTimestamp(1, java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
                statement.setString(2,"topic_prefix");
                statement.setString(3,"topic_version");
                statement.setString(4, HfpMetadata.JourneyType.journey.toString());
                statement.setBoolean(5,true);
                statement.setString(6, HfpMetadata.TransportMode.bus.toString());
                statement.setInt(7, 0);
                statement.setInt(8, 1234);
                statement.setString(9, "1234");

                statement.setInt(10, message.VP.veh);
                statement.setTimestamp(11, java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
                statement.setLong(12, message.VP.tsi);

                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        }
        catch (Exception e) {
            log.error("Failed to insert batch to database: ", e);
            connection.rollback();
            throw e;
        }
        finally {
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Total insert time: {} ms", elapsed);
        }
    }

}
