package fi.hsl.transitlog.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface IMqttMessageHandler {
    void handleMessage(String topic, MqttMessage message) throws Exception;
}
