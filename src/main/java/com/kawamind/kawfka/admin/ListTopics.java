package com.kawamind.kawfka.admin;

import com.kawamind.kawfka.Kawfka;
import com.kawamind.kawfka.config.Configuration;
import com.kawamind.kawfka.config.ProfileNotFoundException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@CommandLine.Command(description = "Liste les topics d'un brocker", name = "listTopic")
@Slf4j
public class ListTopics implements Runnable {

    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "display a help message")
    private final boolean helpRequested = false;
    @CommandLine.Option(names = { "--create" }, description = "Demande de creation de topic")
    private final boolean create = false;
    @CommandLine.Option(names = { "--delete" }, description = "Demande de suppression de topic")
    private final boolean delete = false;
    @CommandLine.Option(names = { "--toEnd" })
    private final boolean toEnd = false;
    @CommandLine.Option(names = { "--toBeginning" })
    private final boolean toBeginning = false;
    @CommandLine.Option(names = { "--partitionId" })
    private final Integer partitionId = null;
    Map<String, Object> readerProps = new HashMap<>();
    @CommandLine.ParentCommand
    Kawfka kawfka;
    @CommandLine.Option(names = { "-g", "--groupId" }, description = "groupId")
    private String groupId;
    @CommandLine.Option(names = { "-t", "--topic" }, description = "topic")
    private String topic;
    @CommandLine.Option(names = { "--partitions" }, description = "Nombre de partition à créer")
    private int numPartition;
    @CommandLine.Option(names = { "--replication" }, description = "facteur de replication")
    private short replicationFactor;

    private void initConfig() throws IOException, ProfileNotFoundException {
        Configuration configuration = Configuration.getConfiguration();
        readerProps = configuration.getReaderPropsByProfile(kawfka.environement);
        readerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();

            if ((toEnd || toBeginning) && Objects.nonNull(topic)) {
                try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(
                        readerProps)) {

                    Set<TopicPartition> partitions = new HashSet<>();
                    if (Objects.nonNull(partitionId)) {
                        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
                        partitions.add(topicPartition);
                    } else {
                        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                        for (PartitionInfo partitionInfo : partitionInfos) {
                            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                        }
                    }

                    //consumer.assign(partitions);
                    final Map<TopicPartition, OffsetAndMetadata> partitionsAndOffset = new HashMap<>();
                    final BiConsumer<TopicPartition, Long> topicPartitionLongBiConsumer = (topicPartition1, aLong) -> {
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(aLong);
                        partitionsAndOffset.put(topicPartition1, offsetAndMetadata);
                    };

                    if (toEnd)
                        consumer.endOffsets(partitions).forEach(topicPartitionLongBiConsumer);
                    if (toBeginning)
                        consumer.beginningOffsets(partitions).forEach(topicPartitionLongBiConsumer);

                    consumer.commitSync(partitionsAndOffset);
                } catch (Exception e) {
                    log.error("", e);
                }

            }
            try (AdminClient kafkaAdminClient = KafkaAdminClient.create(readerProps)) {
               /* kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                    System.out.println(topicListing.name());
                });*/

                if (Objects.nonNull(groupId) && Objects.nonNull(topic))
                    kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().forEach(
                            (topicPartition, offsetAndMetadata) -> {
                                if (topicPartition.topic().equals(topic))
                                    System.out.println(topicPartition.topic() + " " + topicPartition.partition() + " "
                                                               + offsetAndMetadata.offset());
                            });

                if (Objects.nonNull(topic) && delete) {
                    kafkaAdminClient.deleteTopics(Collections.singleton(topic)).all().get();
                    kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                        if (topic.equals(topicListing.name())) {
                            System.out.println("Le topic est toujours présent");
                        }
                    });
                }

                if (Objects.nonNull(topic) && create) {
                    NewTopic newTopic = new NewTopic(topic, numPartition, replicationFactor);
                    kafkaAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
                    kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                        if (topic.equals(topicListing.name())) {
                            System.out.println("Le topic a bien été créé");
                        }
                    });
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
    }

/*
    public static void main(String[] args) {

        AdminClient kafkaAdminClient = KafkaAdminClient.create(readerProps);

        try {
            kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                System.out.println(topicListing.name());
            });

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }*/

}
