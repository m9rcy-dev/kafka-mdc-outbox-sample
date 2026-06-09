package com.example.kafkaoutbox.crypto;

public interface PayloadCrypto {

    byte[] decrypt(byte[] encryptedBytes);

    byte[] encrypt(byte[] plainBytes);
}
