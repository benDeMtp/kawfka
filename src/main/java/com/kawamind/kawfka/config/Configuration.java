package com.kawamind.kawfka.config;

import com.google.common.base.Strings;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class Configuration {

    public static final String KAFKA_SECURITY_KEYSTORE_PASSWORD_ENV = "kafka_security_keystore-password";
    private static final String DEFAULT_ENV_NAME = "innerDefaultEnvName";
    private static final String defaultName = "/default.properties";
    private static com.kawamind.kawfka.config.Configuration CONFIGURATION = null;
    private final Map<String, Properties> propetiesByProfile = new HashMap();

    Configuration() throws IOException {
        this(com.kawamind.kawfka.config.Configuration.class.getResourceAsStream(defaultName));
    }

    Configuration(String customConfigPath) throws IOException {
        this(new FileInputStream(customConfigPath));
    }

    Configuration(InputStream inputStream) throws IOException {
        Properties allProperties = new Properties();
        allProperties.load(inputStream);

        Properties defaultProperties = new Properties();
        for (Object o : allProperties.keySet()) {
            if (o instanceof String) {
                if (!((String) o).startsWith("%")) {
                    defaultProperties.putIfAbsent(o, allProperties.getProperty(o.toString()));
                } else {
                    String profile = ((String) o).substring(1, ((String) o).indexOf('.'));
                    String propKey = ((String) o).replace("%" + profile + ".", "");
                    if (!propetiesByProfile.containsKey(profile)) {
                        propetiesByProfile.put(profile, new Properties());
                    }
                    propetiesByProfile.get(profile).putIfAbsent(propKey, allProperties.getProperty(o.toString()));
                }
            }
        }
        propetiesByProfile.put("", defaultProperties);

        for (String profile : propetiesByProfile.keySet()) {
            if (!profile.equals("")) {
                for (Object o : propetiesByProfile.get("").keySet()) {
                    if (o instanceof String) {
                        propetiesByProfile.get(profile).putIfAbsent(o, propetiesByProfile.get("").getProperty(
                                o.toString()));
                    }
                }
            }
        }
    }

    public static com.kawamind.kawfka.config.Configuration getConfiguration() throws IOException {
        if (Objects.isNull(CONFIGURATION)) {
            initConfiguration(null);
        }
        return CONFIGURATION;
    }

    public static void initConfiguration(String customConfigFilePath) throws IOException {
        if (Strings.isNullOrEmpty(customConfigFilePath)) {
            log.info("Load default configuration");
            CONFIGURATION = new com.kawamind.kawfka.config.Configuration();
        } else {
            log.info("Load custom configuration");
            CONFIGURATION = new com.kawamind.kawfka.config.Configuration(customConfigFilePath);
        }
    }

    private static String getFromPropOrEnv(String propertyName, String envName) {
        String property = System.getProperty(propertyName);
        if (Objects.isNull(property)) {
            property = System.getenv(envName);
            if (Objects.isNull(property)) {
                property = System.getenv(envName.toUpperCase());
            }
        }
        return property;
    }

    public String getBootstrapServers() {
        return this.getBootstrapServers("");
    }

    public String getSchemaRegistryUrl() {
        return this.getSchemaRegistryUrl("");
    }

    public boolean isSsl() {
        return this.isSsl("");
    }

    public String getKeyStoreLocation() {
        return this.getKeyStoreLocation("");
    }

    public String getKeyStorePassword() {
        return this.getKeyStorePassword("");
    }

    public String getBootstrapServers(final String profile) {
        return propetiesByProfile.get(profile).getProperty("kafka.bootstraps-server");
    }

    public String getSchemaRegistryUrl(final String profile) {
        return propetiesByProfile.get(profile).getProperty("kafka.schema-registry-url");
    }

    public boolean isSsl(final String profile) {
        final String aBoolean = propetiesByProfile.get(profile).getProperty("kafka.security.ssl", "false");
        return Boolean.valueOf(aBoolean);
    }

    public String getKeyStoreLocation(final String profile) {
        return propetiesByProfile.get(profile).getProperty("kafka.security.keystore-location");
    }

    public String getKeyStorePassword(final String profile) {
        return getFromPropOrEnv(profile + ".kafka.security.keystore-password", KAFKA_SECURITY_KEYSTORE_PASSWORD_ENV);
    }

    public Map<String, Object> getReaderPropsByProfile(final String profile) throws ProfileNotFoundException {
        if (propetiesByProfile.containsKey(profile)) {
            Map<String, Object> readerProps = new HashMap<>();
            readerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            readerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            readerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
            readerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            String clientId = propetiesByProfile.get(profile).containsKey("kafka.client-id") ?
                    propetiesByProfile.get(profile).getProperty("kafka.client-id") :
                    "kawfka";
            readerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            readerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                            propetiesByProfile.get(profile).getProperty("kafka.bootstraps-server"));
            readerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            propetiesByProfile.get(profile).getProperty("kafka.schema-registry-url"));
            readerProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, Boolean.FALSE);
            readerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.FALSE);
            if (propetiesByProfile.get(profile).containsKey("kafka.max.poll.records"))
                readerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                                propetiesByProfile.get(profile).getProperty("kafka.max.poll.records"));
            if (propetiesByProfile.get(profile).containsKey("kafka.max.poll.interval.ms"))
                readerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                                propetiesByProfile.get(profile).getProperty("kafka.max.poll.interval.ms"));
            if (this.isSsl(profile)) {
                readerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
                //TODO tester si keyStoreLocation est présent
                readerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.getKeyStoreLocation(profile));
                readerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.getKeyStoreLocation(profile));
                readerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.getKeyStorePassword(profile));
                readerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getKeyStorePassword(profile));
            }
            return readerProps;
        }
        throw new ProfileNotFoundException();
    }

    public Properties filterDefaultProperties(final Properties propertiesToFilter) {
        Properties filteredProperties = new Properties();
        filteredProperties.putAll(propertiesToFilter.entrySet().stream().filter(
                objectObjectEntry -> !(objectObjectEntry.getKey().toString().startsWith("%"))).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        return filteredProperties;
    }

    //String getConfigDirectory()
}
