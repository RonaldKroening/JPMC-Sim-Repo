package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import com.jpmc.midascore.foundation.Transaction;

public class KafkaTransactionSerializer implements Serializer<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Transaction data) {
        try {
            System.out.println("Received Transaction: " +
                    "Sender ID = " + data.getSenderId() +
                    ", Recipient = " + data.getRecipientId() +
                    ", Amount = " + data.getAmount());
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing Transaction object", e);
        }
    }
}
