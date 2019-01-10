package fi.hsl.transitlog.hfp;

import fi.hsl.transitlog.mqtt.IMqttMessageHandler;
import fi.hsl.transitlog.mqtt.MqttApplication;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final List<MqttMessage> queue = new LinkedList<>();

    private MessageProcessor() {
    }

    public static MessageProcessor newInstance(MqttApplication app) throws Exception {
        MessageProcessor processor = new MessageProcessor();
        log.info("MessageProcessor subscribing to MQTT Application");
        app.subscribe(processor);
        return processor;
    }


    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        queue.add(message);
        if (queue.size() % 1000 == 0) {
            log.info("Got messages: " + queue.size());
        }
    }
}
