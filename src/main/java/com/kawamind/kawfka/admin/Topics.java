package com.kawamind.kawfka.admin;

import com.kawamind.kawfka.Kawfka;
import com.kawamind.kawfka.config.Configuration;
import com.kawamind.kawfka.config.ProfileNotFoundException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@CommandLine.Command(description = "Administration d'un topic", name = "topic")
@Slf4j
public class Topics implements Runnable {

    @CommandLine.Option(names = {"--list"}, description = "Renvoi la liste des topics du broker")
    private final boolean list = false;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private final boolean helpRequested = false;
    @Inject
    Configuration configuration;
    @CommandLine.ArgGroup(exclusive = false)
    CreateTopicOption createTopicOption;
    @CommandLine.ArgGroup(exclusive = false)
    DeleteTopicOption deleteTopicOption;
    @CommandLine.ArgGroup(exclusive = false)
    InfoOption infoOption;
    @CommandLine.ArgGroup(exclusive = false)
    SeekOption seekOption;
    Map<String, Object> readerProps = new HashMap<>();
    @CommandLine.ParentCommand
    Kawfka kawfka;
    @CommandLine.Option(names = {"-g", "--groupId"}, description = "groupId")
    private String groupId;

    private void initConfig() throws IOException, ProfileNotFoundException {
        readerProps = configuration.getReaderPropsByProfile();
    }

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();

            if (Objects.nonNull(seekOption)) {
                readerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(
                        readerProps)) {

                    Set<TopicPartition> partitions = new HashSet<>();
                    if (Objects.nonNull(seekOption.partitionId)) {
                        TopicPartition topicPartition = new TopicPartition(seekOption.seek, seekOption.partitionId);
                        partitions.add(topicPartition);
                    } else {
                        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(seekOption.seek);
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

                    if (seekOption.toEnd)
                        consumer.endOffsets(partitions).forEach(topicPartitionLongBiConsumer);
                    if (seekOption.toBeginning)
                        consumer.beginningOffsets(partitions).forEach(topicPartitionLongBiConsumer);

                    consumer.commitSync(partitionsAndOffset);
                } catch (Exception e) {
                    log.error("", e);
                }

            }
            try (AdminClient kafkaAdminClient = KafkaAdminClient.create(readerProps)) {
                if (list) {
                    ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
                    listTopicsOptions.listInternal(false);
                    kafkaAdminClient.listTopics(listTopicsOptions).listings().get().forEach(topicListing -> {
                        if (!topicListing.name().startsWith("_"))
                            System.out.println(topicListing.name());
                    });
                }

                if (Objects.nonNull(infoOption))
                    kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().forEach(
                            (topicPartition, offsetAndMetadata) -> {
                                if (topicPartition.topic().equals(infoOption.info))
                                    System.out.println(topicPartition.topic() + " " + topicPartition.partition() + " "
                                            + offsetAndMetadata.offset());
                            });

                if (Objects.nonNull(deleteTopicOption)) {
                    kafkaAdminClient.deleteTopics(Collections.singleton(deleteTopicOption.delete)).all().get();
                    kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                        if (deleteTopicOption.delete.equals(topicListing.name())) {
                            System.out.println("Le topic est toujours présent");
                        }
                    });
                }

                if (Objects.nonNull(createTopicOption)) {
                    NewTopic newTopic = new NewTopic(createTopicOption.create, createTopicOption.numPartition, createTopicOption.replicationFactor);
                    kafkaAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
                    kafkaAdminClient.listTopics().listings().get().forEach(topicListing -> {
                        if (createTopicOption.create.equals(topicListing.name())) {
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

    static class CreateTopicOption {
        @CommandLine.Option(paramLabel = "TOPIC NAME", names = {"--create", "-c"}, description = "Demande de creation de topic", required = true)
        private String create;
        @CommandLine.Option(names = {"--partitions"}, description = "Nombre de partition à créer", required = true)
        private int numPartition;
        @CommandLine.Option(names = {"--replication"}, description = "facteur de replication", required = true)
        private short replicationFactor;
    }

    static class DeleteTopicOption {
        @CommandLine.Option(paramLabel = "TOPIC NAME", names = {"--delete", "-d"}, description = "Demande de suppression de topic", required = true)
        private String delete;
    }
    /*@CommandLine.Option(names = { "-t", "--topic" }, description = "topic")
    private String topic;*/

    static class InfoOption {
        @CommandLine.Option(paramLabel = "TOPIC NAME", names = {"--info", "-i"}, description = "Liste des information sur un topic", required = true)
        private String info;
    }

    static class SeekOption {
        @CommandLine.Option(names = {"--toEnd"}, required = false)
        private final boolean toEnd = false;
        @CommandLine.Option(names = {"--toBeginning"}, required = false)
        private final boolean toBeginning = false;
        @CommandLine.Option(names = {"--partitionId"}, required = true)
        private final Integer partitionId = null;
        @CommandLine.Option(paramLabel = "TOPIC NAME", names = {"--seek", "-s"}, description = "Déplace l'offset sur une partition d'un topic", required = true)
        private String seek;

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
