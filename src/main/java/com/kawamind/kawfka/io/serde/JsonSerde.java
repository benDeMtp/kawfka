package com.kawamind.kawfka.io.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.Consumer;

public class JsonSerde implements FileSerde {
    @Override
    public void read(String filePath, Schema schema, Consumer<GenericRecord> handler) throws IOException, Exception {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(filePath)) {
            // Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONArray objectList = (JSONArray) obj;
            objectList.forEach(o -> {
                GenericRecord avroRecord = new GenericData.Record(schema);
                try {
                    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(o.toString().getBytes()));
                    final GenericDatumReader<GenericData.Record> objectGenericDatumReader = new GenericDatumReader<>(schema);
                    handler.accept(objectGenericDatumReader.read(null, jsonDecoder));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                handler.accept(avroRecord);
            });

        }
    }

}
