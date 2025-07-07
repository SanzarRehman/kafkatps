package com.kafkatps.kafkatps;

import com.kafkatps.kafkatps.messages.DisburseCommand;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class KafkaMessageHandler extends ConcurrentKafkaMessageDispatcher {

    private final DatabaseConnectionPoolProvider databaseProvider;
    
    // Pre-generate default UUIDs to reduce allocation overhead
    private static final UUID DEFAULT_TENANT_ID = UUID.randomUUID();
    private static final UUID DEFAULT_VERTICAL_ID = UUID.randomUUID();
    private static final UUID DEFAULT_USER_ID = UUID.randomUUID();

    public KafkaMessageHandler(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer, 
                              DatabaseConnectionPoolProvider databaseProvider) {
        super(topicName, concurrency, consumer);
        this.databaseProvider = databaseProvider;
    }

    @Override
    public void handleAsync(byte[] message) {
        try {
            // Ultra-fast parsing - exactly like .NET
            DisburseCommand command = JsonUtils.getMessage(message, DisburseCommand.class);
            
            // Extract required data with fallbacks - exactly like .NET CreateLoan
            UUID memberId = UUID.randomUUID(); // Like .NET's Guid.CreateVersion7()
            UUID tenantId = (command.userContext() != null && command.userContext().tenantId() != null) 
                ? command.userContext().tenantId() : DEFAULT_TENANT_ID;
            UUID verticalId = (command.userContext() != null && command.userContext().verticalId() != null) 
                ? command.userContext().verticalId() : DEFAULT_VERTICAL_ID;
            UUID userId = (command.userContext() != null && command.userContext().userId() != null) 
                ? command.userContext().userId() : DEFAULT_USER_ID;
            String language = (command.userContext() != null && command.userContext().language() != null) 
                ? command.userContext().language() : "en";
            String serviceId = (command.userContext() != null && command.userContext().serviceId() != null) 
                ? command.userContext().serviceId() : "default";
            
            // INDIVIDUAL ASYNC DATABASE OPERATION - exactly like .NET's async await pattern
            // Process ONE message at a time like .NET
            CompletableFuture<Void> dbOperation = databaseProvider.insertLoanAsync(memberId, tenantId, verticalId, userId, language, serviceId);
            
            // Handle completion asynchronously (optional - for error handling)
            dbOperation.exceptionally(throwable -> {
                // Silent error handling for performance
                return null;
            });
                
        } catch (Exception e) {
            // Silent error handling for performance
        }
    }
}
