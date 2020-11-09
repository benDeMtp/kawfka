package com.kawamind.kawfka.io;

import com.kawamind.kawfka.config.Configuration;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class KawfkaCommon {

    protected final Map<String, Object> properties = new HashMap<>();
    @Inject
    protected Configuration configuration;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    protected boolean helpRequested = false;
    @CommandLine.Option(description = "topic to send / read messages", names = {"-t", "--topic"}, required = true)
    protected String topic;

    protected void initConfig() {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, configuration.getSchemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, Boolean.TRUE);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.FALSE);
        if (configuration.isSsl()) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            if (configuration.getKeyStoreLocation().isPresent() && configuration.getKeyStorePassword().isPresent()) {
                properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configuration.getKeyStoreLocation().get());
                properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configuration.getKeyStoreLocation().get());
                properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configuration.getKeyStorePassword().get());
                properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, configuration.getKeyStorePassword().get());
            } else {
                throw new RuntimeException("Security parameters are not set. Verify your KeyStoreLocation and your KeyStorePassword");
            }
        }
    }

}
