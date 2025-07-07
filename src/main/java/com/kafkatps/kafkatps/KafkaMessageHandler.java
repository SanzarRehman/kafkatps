package com.kafkatps.kafkatps;

import com.kafkatps.kafkatps.enti.Loan;
import com.kafkatps.kafkatps.messages.DisburseCommand;
import com.kafkatps.kafkatps.repo.LoanRepository;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

public class KafkaMessageHandler extends ConcurrentKafkaMessageDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);

    private final LoanRepository loanRepository;

    public KafkaMessageHandler(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer, LoanRepository loanRepository) {
        super(topicName, concurrency, consumer);
        this.loanRepository = loanRepository;
    }

    @Override
    public Mono<Void> handleAsync(byte[] message) {
        return parseLoan(message)
                .flatMap(loan -> {
                    logger.info("Processing loan for member: {}", loan.getMemberId());
                    return loanRepository.save(loan);
                })
                .doOnSuccess(savedLoan -> logger.info("Successfully saved loan: {} for member: {}",
                        savedLoan.getId(), savedLoan.getMemberId()))
                .doOnError(error -> logger.error("Failed to save loan", error))
                .onErrorResume(error -> {
                    logger.error("Error in handleAsync, continuing...", error);
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Loan> parseLoan(byte[] message) {
        return Mono.fromCallable(() -> {
            try {
                // Parse the DisburseCommand from the message
                DisburseCommand command = JsonUtils.getMessage(message, DisburseCommand.class);
                logger.debug("Parsed command: {}", command.name());

                // Create loan from command
                Loan loan = new Loan();
                loan.setId(UUID.randomUUID());
                loan.setAmount(10000.0); // Default amount for now
                loan.setNewEntry(true);
                loan.setVersion(0);

                // Use the user context from the command
                if (command.userContext() != null) {
                    loan.assignEntityDefaults(command.userContext());
                    logger.debug("Assigned user context for tenant: {}", command.userContext().tenantId());
                } else {
                    // Fallback defaults if no user context
                    loan.setServiceId("kafka-tps");
                    loan.setCreatedDate(Date.from(Instant.now()));
                    loan.setLastUpdatedDate(Date.from(Instant.now()));
                    loan.setCreatedBy(UUID.randomUUID());
                    loan.setLastUpdatedBy(UUID.randomUUID());
                    loan.setTenantId(UUID.randomUUID());
                    loan.setVerticalId(UUID.randomUUID());
                    loan.setLanguage("en");
                    loan.setMarkedToDelete(false);
                    logger.debug("Applied fallback defaults");
                }

                // Set member ID from correlation ID or generate new one
                loan.setMemberId(command.correlationId() != null ? command.correlationId() : UUID.randomUUID());

                return loan;
            } catch (Exception e) {
                logger.error("Failed to parse DisburseCommand from message", e);
                throw new RuntimeException("Failed to parse DisburseCommand", e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
