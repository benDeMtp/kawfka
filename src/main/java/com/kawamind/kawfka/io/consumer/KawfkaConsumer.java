package com.kawamind.kawfka.io.consumer;

import com.kawamind.kawfka.io.KawfkaCommon;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;

import java.io.PrintStream;
import java.util.Arrays;


@CommandLine.Command(description = "Consomme des messages respectant un schéma", name = "consume")
@Slf4j
public class KawfkaConsumer extends KawfkaCommon implements Runnable {

    @CommandLine.Option(description = "fichier de sortie", names = {"-o", "--output"}, required = false)
    protected String output;
    @CommandLine.Option(description = "nombre max d'enregistrement à copier", names = {"-c", "--count"}, required = false)
    protected Integer count;
    KafkaConsumer<String, GenericRecord> kafkaConsumer;

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            kafkaConsumer = new KafkaConsumer(properties);

            kafkaConsumer.assign(Arrays.asList(new TopicPartition(topic, 0),
                    new TopicPartition(topic, 1),
                    new TopicPartition(topic, 2)));

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
