package fi.hsl.transitlog.hfp;

import java.util.List;

public interface IQueueWriter {
    void write(List<HfpMessage> messages) throws Exception;
}
