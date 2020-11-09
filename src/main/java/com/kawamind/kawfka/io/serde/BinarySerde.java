package com.kawamind.kawfka.io.serde;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class BinarySerde implements FileSerde {

    @Override
    public void read(String filePath, Schema schema, Consumer<GenericRecord> handler) throws IOException {
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new File(filePath), new GenericDatumReader<GenericRecord>(schema))) {
            while (reader.hasNext()) {
                final GenericRecord next = reader.next();
                handler.accept(next);
            }
        }
    }
}
