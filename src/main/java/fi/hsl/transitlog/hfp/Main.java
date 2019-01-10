package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.transitlog.mqtt.MqttApplication;
import fi.hsl.transitlog.mqtt.MqttConfig;
import fi.hsl.transitlog.mqtt.MqttConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static MqttConfig createMqttConfig(Config config) {
        String username = "";
        String password = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String usernamePath = ConfigUtils.getEnv("FILEPATH_USERNAME_SECRET").orElse("/run/secrets/mqtt_broker_username");
            log.debug("Reading username from " + usernamePath);
            username = new Scanner(new File(usernamePath)).useDelimiter("\\Z").next();

            final String passwordPath = ConfigUtils.getEnv("FILEPATH_PASSWORD_SECRET").orElse("/run/secrets/mqtt_broker_password");
            log.debug("Reading password from " + passwordPath);
            password = new Scanner(new File(passwordPath)).useDelimiter("\\Z").next();

        } catch (Exception e) {
            log.error("Failed to read secret files", e);
        }

        final String clientId = config.getString("mqtt-broker.clientId");
        final String broker = config.getString("mqtt-broker.host");
        final int maxInFlight = config.getInt("mqtt-broker.maxInflight");
        final String topic = config.getString("mqtt-broker.topic");
        log.info("Setting MQTT topic to {} ", topic);

        MqttConfigBuilder configBuilder = MqttConfig.newBuilder()
                .setBroker(broker)
                .setUsername(username)
                .setPassword(password)
                .setClientId(clientId)
                .setMqttTopic(topic)
                .setMaxInflight(maxInFlight);

        return configBuilder.build();
    }


    public static void main(String[] args) {
        log.info("Launching Transitdata-HFP-Source.");

        Config config = ConfigParser.createConfig();
        MqttConfig mqttConfig = createMqttConfig(config);

        log.info("Configurations read, launching the main loop");

        try (MqttApplication app = MqttApplication.newInstance(mqttConfig)) {

            MessageProcessor processor = MessageProcessor.newInstance(app);
            log.info("Starting to process messages");
            //TODO FIX we cannot use app inside try-with-resources without blocking here, otherwise
            //autocloseable will close itself. now it just seems that paho is unable to close itself
            //it there's any listeners, which has to be fixed also.
        }
        catch (Exception e) {
            log.error("Exception at main", e);
        }

    }
}
