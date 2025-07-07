package com.kafkatps.kafkatps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConcurrentKafkaMessageDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentKafkaMessageDispatcher.class);

    private final String topicName;
    private final int concurrency;
    private final KafkaConsumer<Void, byte[]> consumer;
    private final Map<TopicPartition, TreeSet<Long>> completedOffsetsMap = new HashMap<>();
    private final Map<TopicPartition, AtomicLong> lastCommittedOffsetMap = new HashMap<>();
    private final Object lock = new Object();
    private volatile boolean running = false;

    protected ConcurrentKafkaMessageDispatcher(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.consumer = consumer;
    }

    public void startReactive(AtomicBoolean cancellationToken) {
        logger.info("Starting Kafka reactive dispatcher on topic '{}' with concurrency {}", topicName, concurrency);

        running = true;

        List<TopicPartition> partitions = consumer.partitionsFor(topicName).stream()
                .map(info -> new TopicPartition(topicName, info.partition()))
                .toList();

        consumer.assign(partitions);
        logger.info("Assigned {} partitions: {}", partitions.size(), partitions);

        // Initialize offset tracking for each partition
        synchronized (lock) {
            for (TopicPartition partition : partitions) {
                completedOffsetsMap.put(partition, new TreeSet<>());
                lastCommittedOffsetMap.put(partition, new AtomicLong(-1));
            }
        }

        // Create a single scheduler for Kafka operations to ensure thread safety
        var kafkaScheduler = Schedulers.newSingle("kafka-consumer");

        // Start periodic offset commit
        Flux.interval(Duration.ofSeconds(30))
                .takeWhile(tick -> running && !cancellationToken.get())
                .subscribeOn(kafkaScheduler)
                .subscribe(
                    tick -> tryCommitOffsets(partitions),
                    error -> logger.error("Error in offset commit scheduler", error),
                    () -> logger.info("Offset commit scheduler completed")
                );

        // Main processing loop - use single thread for consumer operations
        Flux.interval(Duration.ZERO, Duration.ofMillis(100))
                .takeWhile(tick -> running && !cancellationToken.get())
                .subscribeOn(kafkaScheduler)
                .flatMap(tick -> Mono.fromCallable(() -> {
                    try {
                        ConsumerRecords<Void, byte[]> records = consumer.poll(Duration.ofMillis(100));
                        logger.debug("Polled {} records", records.count());
                        return records;
                    } catch (Exception e) {
                        logger.error("Polling failed", e);
                        return ConsumerRecords.<Void, byte[]>empty();
                    }
                }))
                .filter(records -> !records.isEmpty())
                .flatMapIterable(records -> records)
                .flatMap(record ->
                    handleAsync(record.value())
                        .doOnSuccess(v -> {
                            logger.debug("Successfully processed record at offset {} partition {}",
                                    record.offset(), record.partition());
                            onRecordProcessed(new TopicPartition(record.topic(), record.partition()), record.offset());
                        })
                        .doOnError(e -> logger.error("Error processing record at offset {} partition {}",
                                record.offset(), record.partition(), e))
                        .onErrorResume(e -> {
                            logger.error("Failed to process message, skipping", e);
                            return Mono.empty();
                        }),
                    concurrency)
                .doOnCancel(() -> {
                    logger.info("Consumer cancelled");
                    shutdown(partitions);
                })
                .doOnComplete(() -> {
                    logger.info("Consumer completed");
                    shutdown(partitions);
                })
                .subscribe(
                    next -> {}, // onNext
                    error -> {
                        logger.error("Stream error", error);
                        shutdown(partitions);
                    },
                    () -> logger.info("Stream completed")
                );
    }

    private void onRecordProcessed(TopicPartition partition, long offset) {
        synchronized (lock) {
            TreeSet<Long> completedOffsets = completedOffsetsMap.get(partition);
            if (completedOffsets != null) {
                completedOffsets.add(offset);
            }
        }
    }

    private void tryCommitOffsets(List<TopicPartition> partitions) {
        try {
            synchronized (lock) {
                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                for (TopicPartition partition : partitions) {
                    TreeSet<Long> completedOffsets = completedOffsetsMap.get(partition);
                    AtomicLong lastCommittedOffset = lastCommittedOffsetMap.get(partition);
                    if (completedOffsets != null && lastCommittedOffset != null) {
                        long offset = lastCommittedOffset.get();
                        while (completedOffsets.contains(offset + 1)) {
                            offset++;
                            completedOffsets.remove(offset);
                        }
                        if (offset > lastCommittedOffset.get()) {
                            lastCommittedOffset.set(offset);
                            commitMap.put(partition, new OffsetAndMetadata(offset + 1));
                        }
                    }
                }
                if (!commitMap.isEmpty()) {
                    consumer.commitAsync(commitMap, (offsets, exception) -> {
                        if (exception != null) {
                            logger.error("Async commit failed", exception);
                        } else {
                            logger.debug("Committed offsets {}", offsets);
                        }
                    });
                }
            }
        } catch (Exception e) {
            logger.error("Error in tryCommitOffsets", e);
        }
    }

    private void shutdown(List<TopicPartition> partitions) {
        if (!running) {
            return;
        }

        running = false;

        try {
            logger.info("Shutting down Kafka dispatcher...");

            // Final offset commit
            tryCommitOffsets(partitions);

            // Wait a bit for async commits to complete
            Thread.sleep(1000);

            // Synchronous final commit
            synchronized (lock) {
                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                for (TopicPartition partition : partitions) {
                    AtomicLong lastCommittedOffset = lastCommittedOffsetMap.get(partition);
                    if (lastCommittedOffset != null && lastCommittedOffset.get() >= 0) {
                        commitMap.put(partition, new OffsetAndMetadata(lastCommittedOffset.get() + 1));
                    }
                }
                if (!commitMap.isEmpty()) {
                    consumer.commitSync(commitMap);
                    logger.info("Final sync commit completed");
                }
            }
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                logger.error("Error closing consumer", e);
            }
            logger.info("Kafka dispatcher shutdown complete.");
        }
    }

    public abstract Mono<Void> handleAsync(byte[] message);
}
