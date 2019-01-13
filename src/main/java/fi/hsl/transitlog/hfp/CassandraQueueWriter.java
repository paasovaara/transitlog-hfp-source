package fi.hsl.transitlog.hfp;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class CassandraQueueWriter implements IQueueWriter {
    private static final Logger log = LoggerFactory.getLogger(CassandraQueueWriter.class);

    Session session;
    PreparedStatement preparedStatement;

    private CassandraQueueWriter(Session session) {
        this.session = session;
        preparedStatement = session.prepare(createInsertStatement());
        log.info("Cassandra writer created");
    }

    public static CassandraQueueWriter newInstance(String host, Integer port, String keyspace) throws Exception {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(host, port, keyspace);
        return new CassandraQueueWriter(connector.getSession());
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
                .append("?, ?, ?, ?, ?, ?, ?, ?, ?,")
                .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
                .append(");")
                .toString();
    }


    public void write(List<HfpMessage> messages) throws Exception {
        //TODO max batch size
        log.info("Writing {} rows to database", messages.size());

        long startTime = System.currentTimeMillis();
        try {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            BatchStatement batchStmt = new BatchStatement();
            for (HfpMessage message: messages) {
                int index = 0;
                BoundStatement statement = boundStatement.bind();

                statement.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("UTC"))));
                statement.setString(index++,"topic_prefix");
                statement.setString(index++,"topic_version");
                statement.setString(index++, HfpMetadata.JourneyType.journey.toString());
                statement.setBool(index++,true);
                statement.setString(index++, HfpMetadata.TransportMode.bus.toString());
                statement.setShort(index++, (short)0);
                statement.setInt(index++, 1234);
                statement.setString(index++, "1234");

                //From payload:
                statement.setString(index++, message.VP.desi);
                statement.setShort(index++, safeParseShort(message.VP.dir));
                statement.setShort(index++, message.VP.oper);
                //setNullable(index++, message.VP.desi, Types.VARCHAR, statement);
                //setNullable(index++, safeParseInt(message.VP.dir), Types.INTEGER, statement);
                //setNullable(index++, message.VP.oper, Types.INTEGER, statement);

                statement.setInt(index++, message.VP.veh);
                statement.setTimestamp(index++, QueueWriter.safeParseTimestamp(message.VP.tst));
                statement.setLong(index++, message.VP.tsi);


                /*
                setNullable(index++, message.VP.spd, Types.DOUBLE, statement);
                setNullable(index++, message.VP.hdg, Types.DOUBLE, statement);
                setNullable(index++, message.VP.lat, Types.DOUBLE, statement);
                setNullable(index++, message.VP.longitude, Types.DOUBLE, statement);
                setNullable(index++, message.VP.acc, Types.DOUBLE, statement);
                setNullable(index++, message.VP.dl, Types.INTEGER, statement);
                setNullable(index++, message.VP.odo, Types.DOUBLE, statement);
                setNullable(index++, QueueWriter.safeParseBoolean(message.VP.drst), Types.BOOLEAN, statement);
                setNullable(index++, message.VP.oday, Types.DATE, statement);
                setNullable(index++, message.VP.jrn, Types.INTEGER, statement);
                setNullable(index++, message.VP.line, Types.INTEGER, statement);
                setNullable(index++, safeParseTime(message.VP.start), Types.TIME, statement);
*/
                batchStmt.add(statement);
            }
            session.execute(batchStmt);
        }
        catch (Exception e) {
            log.error("Failed to insert batch to database: ", e);
            throw e;
        }
        finally {
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Total insert time: {} ms", elapsed);
        }
    }

    static Short safeParseShort(String n) {
        if (n == null || n.isEmpty())
            return null;
        else {
            try {
                return Short.parseShort(n);
            }
            catch (NumberFormatException e) {
                log.error("Failed to convert {} to short", n);
                return null;
            }
        }
    }


    public static class CassandraConnector {

        private Cluster cluster;

        private Session session;

        public void connect(String node, Integer port, String keyspace) throws Exception {
            Cluster.Builder b = Cluster.builder().addContactPoint(node);
            if (port != null) {
                b.withPort(port);
            }
            cluster = b.build();

            session = cluster.connect(keyspace);
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            session.close();
            cluster.close();
        }
    }

}
