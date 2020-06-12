package com.kawamind.kawfka.producer;

import com.kawamind.kawfka.config.Configuration;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Command(description = "Produit des données respectant un schéma", name = "produce")
@Slf4j
public class KawaProducer implements Runnable {

    final Map<String, Object> producerProps = new HashMap<>();
    @Option(names = { "-h", "--help" }, usageHelp = true, description = "display a help message")
    private final boolean helpRequested = false;
    @Option(description = "topic to send messages", names = { "-t", "--topic" }, required = true)
    String topic;
    @Option(description = "schema uri", names = { "-s", "--schema" }, required = true)
    String schemaUri;
    @Option(description = "data json file", names = "--jsonFile", required = true)
    String jsonPath;
    @Option(description = "field name used for the key of kafka message", names = "--idField", required = true)
    String idField;
    KafkaProducer producer;

    Schema.Parser parser;
    Schema schema;

    @SneakyThrows
    @Override
    public void run() {
        if (!helpRequested) {
            initConfig();
            parser = new Schema.Parser();
            schema = parser.parse(Files.readString(Path.of(schemaUri)));
            producer = new KafkaProducer(producerProps);

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

    private void initConfig() throws IOException {
        Configuration configuration = Configuration.getConfiguration();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        producerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, configuration.getSchemaRegistryUrl());
        producerProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, Boolean.TRUE);
        producerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.FALSE);
        if (configuration.isSsl()) {
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            //TODO tester si keyStoreLocation est présent
            producerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, configuration.getKeyStoreLocation());
            producerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configuration.getKeyStoreLocation());
            producerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, configuration.getKeyStorePassword());
            producerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, configuration.getKeyStorePassword());
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
        /*int i = 17000;
        final String _15daysAgo = LocalDate.now().minusDays(14).format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        System.out.println(_15daysAgo);
       try {
            while (i > 0) {
                this.send(_15daysAgo);
                i--;
            }
        }
        finally {
            producer.flush();
            producer.close();
        }*/

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

        /*GenericRecord avroRecord = new GenericData.Record(schema);

        String uuid = UUID.randomUUID().toString();
        avroRecord.put("typePrestation","gmReexpédition");
        avroRecord.put("typeClient","typeClient");
        avroRecord.put("prenomClient","prenomClient");
        avroRecord.put("numeroObjetContrat","numeroObjetContrat");
        avroRecord.put("motifReclamation","motifReclamation");
        avroRecord.put("libellePrestationProduit","libellePrestationProduit");
        avroRecord.put("idQL","idQL");
        avroRecord.put("id", uuid);
        avroRecord.put("etat","etat");
        avroRecord.put("emailClient","emailClient");

        avroRecord.put("dateEffet", date);
        avroRecord.put("dateDemandeDepot","25/02/2020");
        avroRecord.put("coRegateEtab","22345");
        avroRecord.put("canal","canal");

        ProducerRecord<String,Object> record = new ProducerRecord<>("rv_dev_crmRequestServices_in_v1",uuid,avroRecord);

        try {
            producer.send(record);
        } catch(SerializationException e) {
            log.error("Can't send message",e.getMessage());
        }*/
    }

}
