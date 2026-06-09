package com.example.kafkaoutbox.crypto;

import com.example.kafkaoutbox.common.BadInputException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Local deterministic crypto boundary for the runnable sample.
 * Replace this with KMS/envelope encryption or your platform crypto service.
 */
@Component
public class LocalPayloadCrypto implements PayloadCrypto {

    private static final String PREFIX = "enc:";

    @Override
    public byte[] decrypt(byte[] encryptedBytes) {
        String encoded = new String(encryptedBytes, StandardCharsets.UTF_8);
        if (!encoded.startsWith(PREFIX)) {
            throw new BadInputException("Input payload is not encrypted with the local sample format");
        }
        try {
            return Base64.getDecoder().decode(encoded.substring(PREFIX.length()));
        } catch (IllegalArgumentException ex) {
            throw new BadInputException("Unable to decrypt input payload: " + ex.getMessage());
        }
    }

    @Override
    public byte[] encrypt(byte[] plainBytes) {
        String encoded = Base64.getEncoder().encodeToString(plainBytes);
        return (PREFIX + encoded).getBytes(StandardCharsets.UTF_8);
    }
}
