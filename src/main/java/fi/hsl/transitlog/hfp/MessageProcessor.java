package fi.hsl.transitlog.hfp;

import fi.hsl.transitlog.mqtt.IMqttMessageHandler;
import fi.hsl.transitlog.mqtt.MqttApplication;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    //TODO consider different type such as LinkedList for performance. insertion is cheap in that, how about doing the insert?
    final ArrayList<HfpMessage> queue;
    final int QUEUE_MAX_SIZE = 100000;
    final MessageParser parser = MessageParser.newInstance();
    final QueueWriter writer;

    private MessageProcessor(QueueWriter writer) {
        queue = new ArrayList<>(QUEUE_MAX_SIZE);
        this.writer = writer;
    }

    public static MessageProcessor newInstance(MqttApplication app, QueueWriter writer) throws Exception {
        MessageProcessor processor = new MessageProcessor(writer);
        log.info("MessageProcessor subscribing to MQTT Application");
        app.subscribe(processor);
        return processor;
    }


    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        if (queue.size() > QUEUE_MAX_SIZE) {
            log.warn("Queue full: " + QUEUE_MAX_SIZE);
            return;
        }

        //HfpMessage hfp = parser.parse(message);
        HfpMessage hfp = parser.safeParse(message);
        queue.add(hfp);
        //queue.add(message);
        if (queue.size() % 1000 == 0) {
            log.debug("Got messages: " + queue.size());
            //log.info(new String(message.getPayload()));


            //log.info(hfp.VP.desi);
        }

        /*
        if (queue.size() > QUEUE_MAX_SIZE) {
            log.warn("Queue full, removing oldest message");
            queue.removeFirst();
        }*/
    }
}
