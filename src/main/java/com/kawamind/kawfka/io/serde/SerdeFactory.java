package com.kawamind.kawfka.io.serde;

public class SerdeFactory {

    public static FileSerde getBinarySerializer() {
        return new BinarySerde();
    }

    public static FileSerde getJsonSerializer() {
        return new JsonSerde();
    }

}
