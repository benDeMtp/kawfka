package com.kawamind.kawfka.io.consumer;

import com.kawamind.kawfka.io.KawfkaCommon;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
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
import picocli.CommandLine;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@CommandLine.Command(description = "Consomme des messages respectant un schéma", name = "consume")
@Slf4j
public class KawfkaConsumer extends KawfkaCommon implements Runnable {

    @CommandLine.Option(description = "fichier de sortie", names = {"-o", "--output"}, required = false)
    protected String output;
    @CommandLine.Option(description = "nombre max d'enregistrement à copier", names = {"-c", "--count"}, required = false)
    protected Integer count;
    KafkaConsumer<String, GenericRecord> kafkaConsumer;

    @CommandLine.Option(description = "groupId à utiliser", names = {"-g", "--groupId"}, required = false)
    private String groupId;

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            if(groupId!=null){
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
                properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            }

            kafkaConsumer = new KafkaConsumer(properties);

            if(groupId == null) {
                try (AdminClient kafkaAdminClient = KafkaAdminClient.create(properties)) {
                    final DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(Collections.singleton(topic));
                    final Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
                    final TopicDescription topicDescription = values.get(topic).get();
                    final List<TopicPartition> partitions = topicDescription.partitions().stream().map(topicPartitionInfo -> new TopicPartition(topic, topicPartitionInfo.partition())).collect(Collectors.toList());
                    kafkaConsumer.assign(partitions);
                }
            }else{
                System.out.println("Subscription");
                kafkaConsumer.subscribe(Collections.singleton(topic));
            }

            PrintStream out = null;
            if (output != null) {
                out = new PrintStream(output);
                System.setOut(out);
            }

            int readRecords = 0;
            try {
                System.out.printf("[\n");
                infinite_loop:
                while (true) {
                    ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        if (count != null && readRecords >= count)
                            break infinite_loop;
                        System.out.printf("%s,\n", record.value());
                        readRecords++;
                    }
                }
            } finally {
                System.out.printf("]\n");
                kafkaConsumer.close();
                if (out != null) {
                    out.close();
                }
            }

        }
    }
}
