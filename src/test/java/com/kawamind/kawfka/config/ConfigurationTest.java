package com.kawamind.kawfka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigurationTest {

    @Test
    public void configurationShouldReturnDefaultConfigWhenNoProfileIsSpecified() throws IOException {
        Configuration configuration = new Configuration();
        assertEquals("http://127.0.0.1:8081", configuration.getSchemaRegistryUrl());
        assertFalse(configuration.isSsl());
        assertEquals("127.0.0.1:9092", configuration.getBootstrapServers());
        assertNull(configuration.getKeyStorePassword());
        assertEquals("", configuration.getKeyStoreLocation());
    }

    @Test
    public void configurationShouldReturnCustomConfigurationIfConfigurationPathIsPresent()
            throws URISyntaxException, IOException {
        Path path = Path.of(this.getClass().getResource("/custom.properties").toURI());
        Configuration configuration = new Configuration(path.toString());
        assertEquals("http://127.0.0.3:8081", configuration.getSchemaRegistryUrl());
        assertTrue(configuration.isSsl());
        assertEquals("127.0.0.3:9092", configuration.getBootstrapServers());
        assertNull(configuration.getKeyStorePassword());
        assertEquals("customLocation", configuration.getKeyStoreLocation());
    }

    @Test
    @DisplayName("Configuration should load configuration that match the given profile")
    public void configurationShouldReturnProfileSpecificCustomConfigurationIfConfigurationPathIsPresent()
            throws URISyntaxException, IOException {
        Path path = Path.of(this.getClass().getResource("/custom.properties").toURI());
        Configuration configuration = new Configuration(path.toString());
        assertEquals("http://127.0.0.7:8083", configuration.getSchemaRegistryUrl("build"));
        assertFalse(configuration.isSsl("build"));
        assertEquals("127.0.0.9:9094", configuration.getBootstrapServers("build"));
        assertNull(configuration.getKeyStorePassword("build"));
        assertEquals("customLocationWithProfile", configuration.getKeyStoreLocation("build"));
    }

    @Test
    @DisplayName("Only properties not preixed by profile name should be returned")
    public void filterDefaultCOnfiguration() throws IOException, URISyntaxException {
        Path path = Path.of(this.getClass().getResource("/custom.properties").toURI());
        Properties allProperties = new Properties();
        allProperties.load(new FileInputStream(path.toString()));
        Configuration configuration = new Configuration(path.toString());
        Properties properties = configuration.filterDefaultProperties(allProperties);
        for (Object o : properties.keySet()) {
            if (o instanceof String) {
                assertFalse(((String) o).startsWith("%"), "should not contains profile prefixed properties : " + o);
            }
        }
        assertTrue(properties.containsKey("kafka.bootstraps-server"));
        assertTrue(properties.containsKey("kafka.schema-registry-url"));
        assertTrue(properties.containsKey("kafka.security.ssl"));
        assertTrue(properties.containsKey("kafka.security.keystore-location"));
    }

    @Test()
    public void getConsumerPropertiesShouldThrowExceptionIfProfileDoesntExist() throws IOException, URISyntaxException {
        Path path = Path.of(this.getClass().getResource("/custom.properties").toURI());
        Configuration configuration = new Configuration(path.toString());
        assertThrows(ProfileNotFoundException.class, () -> configuration.getReaderPropsByProfile("dummy"));
    }

    @Test
    @DisplayName("A reader configuration should be available for given profile")
    public void getReaderConfByProfileShouldReturnMatchingConfiguration()
            throws URISyntaxException, IOException, ProfileNotFoundException {
        Path path = Path.of(this.getClass().getResource("/custom.properties").toURI());
        Configuration configuration = new Configuration(path.toString());
        Map<String, Object> readerProps = configuration.getReaderPropsByProfile("build");

        assertNotNull(readerProps);
        assertEquals(KafkaAvroDeserializer.class, readerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
                     ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        assertEquals(Boolean.FALSE, readerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
                     ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        assertEquals("earliest", readerProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        assertEquals("test-clientID", readerProps.get(ConsumerConfig.CLIENT_ID_CONFIG),
                     ConsumerConfig.CLIENT_ID_CONFIG);
        assertEquals("127.0.0.9:9094", readerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertEquals("http://127.0.0.7:8083", readerProps.get(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                     KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG);
        assertEquals(Boolean.FALSE, readerProps.get(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS));
        assertEquals(Boolean.FALSE, readerProps.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG));
    }

}