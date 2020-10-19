package com.kawamind.kawfka.io.producer;

import com.kawamind.kawfka.io.KawfkaCommon;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

@Command(description = "Produit des messages respectant un schéma", name = "produce")
@Slf4j
public class KawfkaProducer extends KawfkaCommon implements Runnable {


    @CommandLine.Option(description = "schema uri", names = {"-s", "--schema"}, required = true)
    protected String schemaUri;
    @Option(description = "data json file", names = "--jsonFile", required = true)
    private String jsonPath;
    @Option(description = "field name used for the key of kafka message", names = "--idField", required = true)
    private String idField;
    KafkaProducer producer;

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            Schema.Parser parser = new Schema.Parser();
            System.out.println(this.schemaUri);
            schema = parser.parse(Files.readString(Path.of(schemaUri)));
            producer = new KafkaProducer(properties);

            //readJson
            JSONParser jsonParser = new JSONParser();

            try (FileReader reader = new FileReader(jsonPath)) {
                // Read JSON file
                Object obj = jsonParser.parse(reader);

                JSONArray objectList = (JSONArray) obj;

                this.sendBatch(objectList);
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }

        }
    }


    public void sendBatch(JSONArray objectList) {
        objectList.forEach(o -> {
            GenericRecord avroRecord = new GenericData.Record(schema);
            ((Set<Map.Entry<String, Object>>) ((JSONObject) o).entrySet()).stream().forEach(
                    o1 -> avroRecord.put(o1.getKey(), o1.getValue()));
            send(avroRecord);
        });
        producer.flush();
        producer.close();

    }

    public void send(GenericRecord genericRecord) {
        log.info(genericRecord.toString());
        try {
            ProducerRecord<String, Object> record = new ProducerRecord(topic, genericRecord.get(idField),
                                                                       genericRecord);
            producer.send(record);
        } catch (SerializationException e) {
            log.error("Can't send message", e);
        }
    }

}
