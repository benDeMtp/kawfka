package com.kawamind.kawfka.io.producer;

import com.kawamind.kawfka.io.KawfkaCommon;
import com.kawamind.kawfka.io.serde.SerdeFactory;
import io.quarkus.runtime.Quarkus;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Command(description = "Produit des messages respectant un schéma", name = "produce")
@Slf4j
public class KawfkaProducer extends KawfkaCommon implements Runnable {


    @CommandLine.Option(description = "schema uri", names = {"-s", "--schema"}, required = true)
    protected String schemaUri;
    @CommandLine.ArgGroup(heading = "Format du fichier d'entrée")
    Format format;
    @Option(description = "field name used for the key of kafka message", names = "--idField", required = true)
    private String idField;
    KafkaProducer producer;
    @Option(description = "data file", names = "--file", required = true)
    private String filePath;

    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            Schema schema = openSchema(schemaUri);
            producer = new KafkaProducer(properties);
            try {
                if (format.json) {
                    SerdeFactory.getJsonSerializer().read(filePath, schema, genericRecord -> this.send(genericRecord));
                } else {
                    SerdeFactory.getBinarySerializer().read(filePath, schema, genericRecord -> this.send(genericRecord));
                }
            } catch (Exception e) {
                log.error("Enable to write to file", e);
                Quarkus.asyncExit(2);
                Quarkus.waitForExit();
            }
            producer.flush();
            producer.close();
        }
    }

    Schema openSchema(String schamePath) {
        try {
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(Files.readString(Path.of(schemaUri)));
        } catch (IOException e) {
            log.error("Error when reading schema", e);
            Quarkus.asyncExit(1);
            Quarkus.waitForExit();
            return null;
        }
    }

    void send(GenericRecord genericRecord) {
        log.info(genericRecord.toString());
        try {
            ProducerRecord<String, Object> record = new ProducerRecord(topic, String.valueOf(genericRecord.get(idField)),
                    genericRecord);
            producer.send(record);
        } catch (SerializationException e) {
            log.error("Can't send message", e);
        }
    }

    static class Format {
        @CommandLine.Option(description = "binaire", names = {"-b", "--binary"})
        private final Boolean binary = Boolean.FALSE;
        @CommandLine.Option(description = "json, default", names = {"-j", "--json"})
        private final Boolean json = Boolean.FALSE;
    }

}
