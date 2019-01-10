package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class QueueWriter {
    private static final Logger log = LoggerFactory.getLogger(QueueWriter.class);

    Connection connection;

    private QueueWriter(Connection db) {
        connection = db;
    }

    public static QueueWriter newInstance(Config config) throws Exception {
        final String connectionString = config.getString("db.connectionString");
        final String user = config.getString("db.username");
        final String password = config.getString("db.password");

        log.info("Connecting to the database with connection string " + connectionString);
        Connection db = DriverManager.getConnection(connectionString, user, password);
        log.info("Connection success");
        return new QueueWriter(db);
    }

    public void write(List<HfpMessage> messages) throws Exception {

    }

    protected ResultSet performQuery(PreparedStatement statement) throws SQLException {
        long queryStartTime = System.currentTimeMillis();
        ResultSet resultSet = statement.executeQuery();
        long elapsed = System.currentTimeMillis() - queryStartTime;
        log.info("Total query and processing time: {} ms", elapsed);
        return resultSet;
    }
}
