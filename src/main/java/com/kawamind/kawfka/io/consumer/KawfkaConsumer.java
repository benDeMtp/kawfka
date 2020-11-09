package com.kawamind.kawfka.io.consumer;

import com.kawamind.kawfka.io.KawfkaCommon;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.graalvm.collections.Pair;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@CommandLine.Command(description = "Consomme des messages respectant un schéma", name = "consume")
@Slf4j
public class KawfkaConsumer extends KawfkaCommon implements Runnable {

    @CommandLine.Option(description = "fichier de sortie", names = {"-o", "--output"})
    protected String output;

    KafkaConsumer<String, GenericRecord> kafkaConsumer;

    @CommandLine.ArgGroup()
    Exclusive exclusive;

    @CommandLine.ArgGroup(heading = "Format de sortie")
    Format format;
    PrintStream out = null;

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            if (exclusive.groupId != null) {
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, exclusive.groupId);
            }

            kafkaConsumer = new KafkaConsumer(properties);
            Map<TopicPartition, Long> topicPartitionLastOffset = null;

            if (exclusive.groupId == null) {
                try (AdminClient kafkaAdminClient = KafkaAdminClient.create(properties)) {
                    final DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(Collections.singleton(topic));
                    final Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
                    final TopicDescription topicDescription = values.get(topic).get();
                    final List<TopicPartition> assignment = topicDescription.partitions().stream().map(topicPartitionInfo -> new TopicPartition(topic, topicPartitionInfo.partition())).collect(Collectors.toList());
                    kafkaConsumer.assign(assignment);
                    if (exclusive.lastMessages != 0L) {
                        System.out.println("Récupération des " + exclusive.lastMessages + " derniers messages");
                        topicPartitionLastOffset = kafkaConsumer.endOffsets(assignment);
                        final Map<TopicPartition, Long> topicPartitionFirstOffset = kafkaConsumer.beginningOffsets(assignment);
                        final Map<TopicPartition, Long> targetOffsets = topicPartitionLastOffset.entrySet().stream().map(topicPartitionLongEntry -> {
                            long offset = topicPartitionLongEntry.getValue() - exclusive.lastMessages;
                            if (offset < topicPartitionFirstOffset.get(topicPartitionLongEntry.getKey())) {
                                offset = topicPartitionFirstOffset.get(topicPartitionLongEntry.getKey());
                            }
                            log.info("Nouvel offset " + offset + " pour la partition " + topicPartitionLongEntry.getKey().partition());
                            return Pair.create(topicPartitionLongEntry.getKey(), offset);
                        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
                        targetOffsets.forEach((topicPartition, aLong) -> kafkaConsumer.seek(topicPartition, aLong));
                    }
                }
            } else {
                System.out.println("Subscription");
                kafkaConsumer.subscribe(Collections.singleton(topic));
            }

            if (output == null) {
                out = System.out;
            } else {
                out = new PrintStream(output);
            }
            if (format.binary) {
                toBytes();
            } else {
                toJson();
            }
        }
    }

    void toJson() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            out.println("\n]");
            try {
                kafkaConsumer.close();
            } catch (Exception e) {
                log.error("", e);
            }
            if (out != null) {
                out.close();
            }
        }));
        int readRecords = 0;
        out.printf("[\n");
        try {
            while ((exclusive.count != null && readRecords < exclusive.count) || (exclusive.lastMessages != null && readRecords < (kafkaConsumer.assignment().size() * exclusive.lastMessages))) {
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    ByteArrayOutputStream stream = new ByteArrayOutputStream();
                    if (readRecords > 0) {
                        stream.write(",\n".getBytes());
                    }
                    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(
                            record.value().getSchema(), stream);
                    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.value().getSchema());
                    writer.write(record.value(), jsonEncoder);
                    jsonEncoder.flush();
                    out.printf("%s", stream.toString());
                    stream.close();
                    readRecords++;
                }
            }
        } catch (Exception excpetion) {
            log.error("", excpetion);
        }
    }

    void toBytes() {
        int readRecords = 0;
        DataFileWriter<GenericRecord> writer = null;
        try {
            while ((exclusive.count != null && readRecords < exclusive.count) || (exclusive.lastMessages != null && readRecords < (kafkaConsumer.assignment().size() * exclusive.lastMessages))) {
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    if (readRecords == 0) {
                        writer = new DataFileWriter(new GenericDatumWriter<>(record.value().getSchema()));
                        writer.create(record.value().getSchema(), out);
                    }
                    writer.append(record.value());
                    readRecords++;
                }
            }
        } catch (Exception excpetion) {
            log.error("", excpetion);
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    static class Exclusive {
        @CommandLine.Option(description = "nombre max d'enregistrement à copier", names = {"-c", "--count"})
        protected Integer count;
        @CommandLine.Option(description = "groupId à utiliser", names = {"-g", "--groupId"})
        private String groupId;
        @CommandLine.Option(description = "N dernier message de chaque partitions", names = {"-l", "--lastMessages"})
        private Long lastMessages;
    }

    static class Format {
        @CommandLine.Option(description = "binaire", names = {"-b", "--binary"})
        private final Boolean binary = Boolean.FALSE;
        @CommandLine.Option(description = "json, default", names = {"-j", "--json"})
        private final Boolean json = Boolean.FALSE;
    }
}
