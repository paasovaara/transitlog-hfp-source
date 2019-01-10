package fi.hsl.transitlog.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttApplication implements AutoCloseable, MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttApplication.class);

    private MqttConfig config;
    private MqttAsyncClient mqttClient;
    private String mqttTopic;

    //TODO READ QOS FROM CONFIG
    public static final int DEFAULT_QOS = 1;


    MqttApplication(MqttAsyncClient client, String topic) {
        this.mqttClient = client;
        this.mqttTopic = topic;
        mqttClient.setCallback(this);//TODO move to happen before connect so we won't lose messages
    }

    public MqttConfig getConfig() {
        return config;
    }

    public static MqttApplication newInstance(MqttConfig config) throws Exception {
        MqttAsyncClient mqttClient = null;
        try {
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(false); //WHY FALSE? WHY NOT TRUE?
            connectOptions.setMaxInflight(config.getMaxInflight());
            connectOptions.setAutomaticReconnect(false); //Let's abort on connection errors

            connectOptions.setUserName(config.getUsername());
            connectOptions.setPassword(config.getPassword().toCharArray());

            //Let's use memory persistance to optimize throughput.
            MemoryPersistence memoryPersistence = new MemoryPersistence();

            mqttClient = new MqttAsyncClient(config.getBroker(), config.getClientId(), memoryPersistence);

            log.info(String.format("Connecting to mqtt broker %s", config.getBroker()));
            IMqttToken token = mqttClient.connect(connectOptions, null, new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Connected");
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log.error("Connection failed: ", exception);
                }
            });
            token.waitForCompletion();

            log.info("Connection to MQTT finished");
        }
        catch (Exception e) {
            log.error("Error connecting to MQTT", e);
            if (mqttClient != null) {
                //Paho doesn't close the connection threads unless we force-close it.
                mqttClient.close(true);
            }
            throw e;
        }
        return new MqttApplication(mqttClient, config.getMqttTopic());
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("Connection to mqtt broker lost", cause);
        close();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        //TODO possibly queue messages here and offer some loop
        //TODO think about the need for this abstraction

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    public void subscribe(final IMqttMessageHandler handler) throws Exception {

        mqttClient.subscribe(mqttTopic, DEFAULT_QOS, new IMqttMessageListener() {
            //TODO think about this, perhaps some error handling could be in place
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                handler.handleMessage(topic, message);
            }
        });
    }

    public void close() {
        log.info("Closing MqttApplication resources");
        try {
            //Paho doesn't close the connection threads unless we force-close it.
            //mqttClient.unsubscribe(mqttTopic);
            //mqttClient.disconnect();
            mqttClient.disconnectForcibly(5000L);
            mqttClient.close(true);
        }
        catch (Exception e) {
            log.error("Failed to close MQTT client connection", e);
        }
    }
}
