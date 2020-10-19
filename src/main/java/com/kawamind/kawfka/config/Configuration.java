package com.kawamind.kawfka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@ApplicationScoped
@Data
public class Configuration {

    @ConfigProperty(name = "kafka.bootstrap-servers")
    String bootstrapServers;

    @ConfigProperty(name = "kafka.schema-registry-url")
    String schemaRegistryUrl;

    @ConfigProperty(name = "kafka.security.keystore-location")
    Optional<String> keyStoreLocation;

    @ConfigProperty(name = "kafka.security.keystore-password")
    Optional<String> keyStorePassword;

    @ConfigProperty(name = "kafka.security.ssl", defaultValue = "false")
    boolean ssl;

    @ConfigProperty(name = "kafka.client-id", defaultValue = "kawfka")
    String clientId;

    @ConfigProperty(name = "kafka.consumer.max.poll.records")
    Optional<String> maxPollRecord;

    @ConfigProperty(name = "kafka.consumer.max.poll.interval.ms")
    Optional<String> maxPollIntervalMs;

    public Map<String, Object> getReaderPropsByProfile() throws ProfileNotFoundException {
        Map<String, Object> readerProps = new HashMap<>();
        readerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        readerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        readerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        readerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        readerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        readerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        readerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        readerProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, Boolean.FALSE);
        readerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.FALSE);
        if (maxPollRecord.isPresent())
            readerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                    maxPollRecord.get());
        if (maxPollIntervalMs.isPresent())
            readerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                    maxPollIntervalMs.get());
        if (isSsl()) {
            readerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            if (getKeyStoreLocation().isPresent()) {
                readerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.getKeyStoreLocation().get());
                readerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.getKeyStoreLocation().get());
                if (getKeyStorePassword().isPresent()) {
                    readerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.getKeyStorePassword().get());
                    readerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getKeyStorePassword().get());
                } else {
                    throw new RuntimeException("Please provide a password for the keystore");
                }
            } else {
                throw new RuntimeException("Please provide valide certificate");
            }

        }
        for (String s : readerProps.keySet()) {
            System.out.println(s + " : " + readerProps.get(s));
        }
        return readerProps;
    }

}
