package fi.hsl.transitlog.hfp;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;

public class MessageParser {
    // Let's use dsl-json (https://github.com/ngs-doo/dsl-json) for performance.
    // Based on this benchmark: https://github.com/fabienrenaud/java-json-benchmark

    //Example: https://github.com/ngs-doo/dsl-json/blob/master/examples/MavenJava8/src/main/java/com/dslplatform/maven/Example.java

    //Note! Apparently not thread safe, for per thread reuse use ThreadLocal pattern or create separate instances
    final DslJson<Object> dslJson = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());

    public static MessageParser newInstance() {
        return new MessageParser();
    }

    public HfpMessage parse(MqttMessage message) throws IOException {
        byte[] data = message.getPayload();
        return dslJson.deserialize(HfpMessage.class, data, data.length);
    }
}
