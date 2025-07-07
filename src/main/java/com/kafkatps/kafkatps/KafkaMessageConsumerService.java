package com.kafkatps.kafkatps;

import com.kafkatps.kafkatps.repo.LoanRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMessageConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumerService.class);

    private final String topicName;
    private final int concurrency;
    private final String bootstrapServers;
    private final String groupId;
    private KafkaConsumer<Void, byte[]> consumer;
    private KafkaMessageHandler handler;
    private AtomicBoolean cancellationToken;
    private final LoanRepository loanRepository;

    public KafkaMessageConsumerService(
            String topicName,
            int concurrency,
            String bootstrapServers,
            String groupId,
            LoanRepository loanRepository
    ) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.loanRepository = loanRepository;
    }

    public void start() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000");
        consumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576");
        consumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        consumer = new KafkaConsumer<>(consumerConfig);

        handler = new KafkaMessageHandler(topicName, concurrency, consumer, loanRepository);
        cancellationToken = new AtomicBoolean(false);
        handler.startReactive(cancellationToken); // returns void

        logger.info("Started Kafka consumer for topic '{}' with concurrency {}", topicName, concurrency);
    }

    public void stop() {
        logger.info("Stopping Kafka consumer...");
        if (cancellationToken != null) {
            cancellationToken.set(true);
        }
        logger.info("Kafka consumer stopped.");
    }
}
