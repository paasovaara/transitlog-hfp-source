package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import static java.sql.Types.*;

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
                .append("desi, dir, oper,")
                .append("veh, tst, tsi,")
                .append("spd, hdg, lat,")
                .append("long, acc, dl,")
                .append("odo, drst, oday,")
                .append("jrn, line, start")
                .append(") VALUES (")
                .append("?, ?, ?, ?::JOURNEY_TYPE, ?, ?::TRANSPORT_MODE, ?, ?, ?,")
                .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
                .append(");")
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

                //From payload:
                setNullable(10, message.VP.desi, Types.VARCHAR, statement);
                setNullable(11, safeParseInt(message.VP.dir), Types.INTEGER, statement);
                setNullable(12, message.VP.oper, Types.INTEGER, statement);

                statement.setInt(13, message.VP.veh);
                statement.setTimestamp(14, safeParseTimestamp(message.VP.tst));
                statement.setLong(15, message.VP.tsi);

                setNullable(16, message.VP.spd, Types.DOUBLE, statement);
                setNullable(17, message.VP.hdg, Types.DOUBLE, statement);
                setNullable(18, message.VP.lat, Types.DOUBLE, statement);
                setNullable(19, message.VP.longitude, Types.DOUBLE, statement);
                setNullable(20, message.VP.acc, Types.DOUBLE, statement);
                setNullable(21, message.VP.dl, Types.INTEGER, statement);
                setNullable(22, message.VP.odo, Types.DOUBLE, statement);
                setNullable(23, safeParseBoolean(message.VP.drst), Types.BOOLEAN, statement);
                setNullable(24, message.VP.oday, Types.DATE, statement);
                setNullable(25, message.VP.jrn, Types.INTEGER, statement);
                setNullable(26, message.VP.line, Types.INTEGER, statement);
                setNullable(27, safeParseTime(message.VP.start), Types.TIME, statement);

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

    private void setNullable(int index, Object value, int jdbcType, PreparedStatement statement) throws SQLException {
        if (value == null) {
            statement.setNull(index, jdbcType);
        }
        else {
            //This is just awful but Postgres driver does not support setObject(value, type);
            //Leaving null values not set is also not an option.
            switch (jdbcType) {
                case Types.BOOLEAN: statement.setBoolean(index, (Boolean)value);
                    break;
                case Types.INTEGER: statement.setInt(index, (Integer) value);
                    break;
                case Types.BIGINT: statement.setLong(index, (Long)value);
                    break;
                case Types.DOUBLE: statement.setDouble(index, (Double) value);
                    break;
                case Types.DATE: statement.setDate(index, (Date)value);
                    break;
                case Types.TIME: statement.setTime(index, (Time)value);
                    break;
                case Types.VARCHAR: statement.setString(index, (String)value); //Not sure if this is correct, field in schema is TEXT
                    break;
                default: log.error("Invalid jdbc type, bug in the app! {}", jdbcType);
                    break;
            }
        }
    }

    static Integer safeParseInt(String n) {
        if (n == null || n.isEmpty())
            return null;
        else {
            try {
                return Integer.parseInt(n);
            }
            catch (NumberFormatException e) {
                log.error("Failed to convert {} to integer", n);
                return null;
            }
        }
    }

    static Boolean safeParseBoolean(Integer n) {
        if (n == null)
            return null;
        else
            return n != 0;
    }

    static Time safeParseTime(String time) {
        if (time == null)
            return null;
        else {
            try {
                return Time.valueOf(time + ":00"); // parser requires seconds also.
            }
            catch (Exception e) {
                log.error("Failed to convert {} to java.sql.Time", time);
                return null;
            }
        }
    }

    static Timestamp safeParseTimestamp(String dt) {
        if (dt == null)
            return null;
        else {
            try {
                OffsetDateTime offsetDt = OffsetDateTime.parse(dt);
                return new Timestamp(offsetDt.toEpochSecond() * 1000L);
            }
            catch (Exception e) {
                log.error("Failed to convert {} to java.sql.Timestamp", dt);
                return null;
            }
        }
    }
}
