package com.example.kafkaoutbox.crypto;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class EncryptingByteArraySerializer implements Serializer<byte[]> {

    private final PayloadCrypto payloadCrypto;

    public EncryptingByteArraySerializer(PayloadCrypto payloadCrypto) {
        this.payloadCrypto = payloadCrypto;
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        return encrypt(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, byte[] data) {
        return encrypt(data);
    }

    private byte[] encrypt(byte[] data) {
        if (data == null) {
            return null;
        }
        return payloadCrypto.encrypt(data);
    }
}
