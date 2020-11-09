package com.kawamind.kawfka.io.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.function.Consumer;

public interface FileSerde {

    void read(String filePath, Schema schema, Consumer<GenericRecord> handler) throws Exception;

}
