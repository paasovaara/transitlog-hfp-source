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
    PreparedStatement statement;

    private QueueWriter(Connection conn, PreparedStatement statement) {
        connection = conn;
        this.statement = statement;
    }

    public static QueueWriter newInstance(Config config) throws Exception {
        final String connectionString = config.getString("db.connectionString");
        final String user = config.getString("db.username");
        final String password = config.getString("db.password");

        log.info("Connecting to the database with connection string " + connectionString);
        Connection conn = DriverManager.getConnection(connectionString, user, password);
        String queryString = createInsertStatement();
        PreparedStatement statement = conn.prepareStatement(queryString);
        conn.setAutoCommit(false); // we're doing batch inserts so no auto commit
        log.info("Connection success");
        return new QueueWriter(conn, statement);
    }

    private static String createInsertStatement() {
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
        try {
            for (HfpMessage message: messages) {
                int index = 1;
                statement.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
                statement.setString(index++,"topic_prefix");
                statement.setString(index++,"topic_version");
                statement.setString(index++, HfpMetadata.JourneyType.journey.toString());
                statement.setBoolean(index++,true);
                statement.setString(index++, HfpMetadata.TransportMode.bus.toString());
                statement.setInt(index++, 0);
                statement.setInt(index++, 1234);
                statement.setString(index++, "1234");

                //From payload:
                setNullable(index++, message.VP.desi, Types.VARCHAR, statement);
                setNullable(index++, safeParseInt(message.VP.dir), Types.INTEGER, statement);
                setNullable(index++, message.VP.oper, Types.INTEGER, statement);

                statement.setInt(index++, message.VP.veh);
                statement.setTimestamp(index++, safeParseTimestamp(message.VP.tst));
                statement.setLong(index++, message.VP.tsi);

                setNullable(index++, message.VP.spd, Types.DOUBLE, statement);
                setNullable(index++, message.VP.hdg, Types.DOUBLE, statement);
                setNullable(index++, message.VP.lat, Types.DOUBLE, statement);
                setNullable(index++, message.VP.longitude, Types.DOUBLE, statement);
                setNullable(index++, message.VP.acc, Types.DOUBLE, statement);
                setNullable(index++, message.VP.dl, Types.INTEGER, statement);
                setNullable(index++, message.VP.odo, Types.DOUBLE, statement);
                setNullable(index++, safeParseBoolean(message.VP.drst), Types.BOOLEAN, statement);
                setNullable(index++, message.VP.oday, Types.DATE, statement);
                setNullable(index++, message.VP.jrn, Types.INTEGER, statement);
                setNullable(index++, message.VP.line, Types.INTEGER, statement);
                setNullable(index++, safeParseTime(message.VP.start), Types.TIME, statement);

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
