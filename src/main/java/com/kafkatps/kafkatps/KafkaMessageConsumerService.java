package com.kafkatps.kafkatps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMessageConsumerService {

    private final String topicName;
    private final int concurrency;
    private final String bootstrapServers;
    private final String groupId;
    private KafkaConsumer<Void, byte[]> consumer;
    private KafkaMessageHandler handler;
    private AtomicBoolean cancellationToken;
    private final DatabaseConnectionPoolProvider databaseProvider;

    public KafkaMessageConsumerService(
            String topicName,
            int concurrency,
            String bootstrapServers,
            String groupId,
            DatabaseConnectionPoolProvider databaseProvider
    ) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.databaseProvider = databaseProvider;
    }

    public void start() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerConfig.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        
        // .NET-style configuration - matching the C# consumer exactly
        consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        consumer = new KafkaConsumer<>(consumerConfig);
        
        // Use the new high-performance database provider
        handler = new KafkaMessageHandler(topicName, concurrency, consumer, databaseProvider);
        cancellationToken = new AtomicBoolean(false);
        handler.startReactive(cancellationToken);
    }

    public void stop() {
        if (cancellationToken != null) {
            cancellationToken.set(true);
        }
        if (handler != null) {
            handler.shutdown();
        }
        if (databaseProvider != null) {
            databaseProvider.close();
        }
    }
}
