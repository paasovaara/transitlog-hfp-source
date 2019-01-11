package fi.hsl.transitlog.hfp;

import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QueueWriterTest {
    @Test
    public void parseTimestampSafely() {
        Timestamp ts = QueueWriter.safeParseTimestamp("2018-04-05T17:38:36Z");
        assertEquals(1522949916000L, ts.getTime());

        Timestamp missingTimezone = QueueWriter.safeParseTimestamp("2018-04-05T17:38:36");
        assertNull(missingTimezone);

        assertNull(QueueWriter.safeParseTimestamp("datetime"));
        assertNull(QueueWriter.safeParseTimestamp(null));
    }

    @Test
    public void parseTimeSafely() {
        Time time = QueueWriter.safeParseTime("18:00");
        assertTrue(time.toLocalTime().equals(LocalTime.of(18, 0)));

        Time earlyTime = QueueWriter.safeParseTime("8:00");
        assertTrue(earlyTime.toLocalTime().equals(LocalTime.of(8, 0)));

        Time earlyTime2 = QueueWriter.safeParseTime("08:00");
        assertTrue(earlyTime2.toLocalTime().equals(LocalTime.of(8, 0)));

        assertNull(QueueWriter.safeParseTime("random-time"));
        assertNull(QueueWriter.safeParseTime(null));
    }

}
