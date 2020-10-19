package com.kawamind.kawfka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ConfigurationTest {

    @Inject
    Configuration configuration;

    @Test
    public void configurationShouldReturnDefaultConfigWhenNoProfileIsSpecified() throws IOException {
        assertEquals("http://127.0.0.7:8083", configuration.getSchemaRegistryUrl());
        assertFalse(configuration.isSsl());
        assertEquals("127.0.0.9:9094", configuration.getBootstrapServers());
        assertTrue(configuration.getKeyStorePassword().isEmpty());
        assertTrue(configuration.getKeyStoreLocation().isPresent());
    }


    @Test
    @DisplayName("Configuration should load configuration that match the given profile")
    public void configurationShouldReturnProfileSpecificCustomConfigurationIfConfigurationPathIsPresent()
            throws URISyntaxException, IOException {
        assertEquals("http://127.0.0.7:8083", configuration.getSchemaRegistryUrl());
        assertFalse(configuration.isSsl());
        assertEquals("127.0.0.9:9094", configuration.getBootstrapServers());
        assertTrue(configuration.getKeyStorePassword().isEmpty());
        assertEquals("customLocationWithProfile", configuration.getKeyStoreLocation().get());
    }


    @Test
    @DisplayName("A reader configuration should be available for given profile")
    public void getReaderConfByProfileShouldReturnMatchingConfiguration()
            throws URISyntaxException, IOException, ProfileNotFoundException {
        Map<String, Object> readerProps = configuration.getReaderPropsByProfile();

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