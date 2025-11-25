package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // Log with Spring logger (guaranteed to show in Maven/terminal)
        logger.info("🟢 Received transaction: {}", transaction.getAmount());

        // Optional: also print to console and flush for extra safety
        System.out.println("🟢 Received transaction: " + transaction.getAmount());
        System.out.flush();

        // Optional tiny sleep to ensure ordering of console output
        try {
            Thread.sleep(20); // 20ms
        } catch (InterruptedException ignored) {}
    }
}