package com.kafkatps.kafkatps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConcurrentKafkaMessageDispatcher {

    private final String topicName;
    private final int concurrency;
    private final KafkaConsumer<Void, byte[]> consumer;
    
    // Exact .NET architecture - TreeSet for ordered offset tracking
    private final TreeSet<Long> completedOffsets = new TreeSet<>();
    private final AtomicLong lastCommittedOffset = new AtomicLong(-1);
    private final Object locker = new Object();
    
    // Bounded channel with EXACT capacity like .NET - critical for backpressure
    private final BlockingQueue<ConsumerRecord<Void, byte[]>> channel;
    private final ExecutorService workerPool;
    private volatile boolean running = false;
    private TopicPartition topicPartition;
    
    // Thread-safe commit tracking
    private volatile long lastCommitTime = System.currentTimeMillis();
    private static final long COMMIT_INTERVAL_MS = 1000; // 1 second like .NET

    protected ConcurrentKafkaMessageDispatcher(String topicName, int concurrency, KafkaConsumer<Void, byte[]> consumer) {
        this.topicName = topicName;
        this.concurrency = concurrency;
        this.consumer = consumer;
        
        // Bounded channel with exact capacity like .NET - prevents memory issues
        this.channel = new LinkedBlockingQueue<>(concurrency);
        
        // Create fixed thread pool for workers - exactly like .NET's Task.WhenAll
        this.workerPool = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("KafkaWorker-" + t.getId());
            return t;
        });
    }

    public abstract void handleAsync(byte[] message);

    public void startReactive(AtomicBoolean cancellationToken) {
        running = true;
        
        // Subscribe to topic (like .NET)
        consumer.subscribe(Collections.singletonList(topicName));
        
        // Start main consumer thread - this is the core polling loop
        workerPool.submit(() -> {
            boolean workersStarted = false;
            
            try {
                while (!cancellationToken.get() && running) {
                    ConsumerRecords<Void, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    
                    // Only start workers when we actually have messages - lazy initialization
                    if (!records.isEmpty() && !workersStarted) {
                        // Initialize offset tracking with first message
                        ConsumerRecord<Void, byte[]> firstRecord = records.iterator().next();
                        lastCommittedOffset.set(firstRecord.offset() - 1);
                        this.topicPartition = new TopicPartition(firstRecord.topic(), firstRecord.partition());
                        
                        // Now start worker threads - exactly like .NET's DispatchMessagesAsync
                        startWorkerThreads();
                        workersStarted = true;
                    }
                    
                    // Process messages only if workers are started
                    if (workersStarted) {
                        for (ConsumerRecord<Void, byte[]> record : records) {
                            if (topicPartition == null) {
                                topicPartition = new TopicPartition(record.topic(), record.partition());
                            }
                            
                            // Blocking put with timeout - provides natural backpressure like .NET's bounded channel
                            while (running && !channel.offer(record, 1, TimeUnit.SECONDS)) {
                                // Backpressure - wait
                            }
                        }
                    }
                    
                    // THREAD-SAFE COMMIT: Handle commits on the same thread as polling
                    if (workersStarted && System.currentTimeMillis() - lastCommitTime > COMMIT_INTERVAL_MS) {
                        commitOffsetsOnConsumerThread();
                        lastCommitTime = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // Silent error handling for performance
            } finally {
                // Send shutdown markers to all workers only if they were started
                if (workersStarted) {
                    for (int i = 0; i < concurrency; i++) {
                        try {
                            channel.offer(createShutdownMarker(), 1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    
                    // Final commit on consumer thread
                    commitOffsetsOnConsumerThread();
                }
            }
        });
    }

    private void startWorkerThreads() {
        for (int i = 0; i < concurrency; i++) {
            final int workerId = i;
            workerPool.submit(() -> {
                while (running && !Thread.currentThread().isInterrupted()) {
                    try {
                        ConsumerRecord<Void, byte[]> record = channel.poll(1, TimeUnit.SECONDS);
                        
                        if (record != null) {
                            // Check for shutdown marker
                            if (record.offset() == -1) {
                                break;
                            }
                            
                            try {
                                // Process message - exactly like .NET's HandleAsync
                                handleAsync(record.value());
                                
                                // Mark as completed for offset management - exactly like .NET
                                synchronized (locker) {
                                    completedOffsets.add(record.offset());
                                }
                                
                            } catch (Exception e) {
                                // Silent error handling for performance
                            }
                        }
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        // Silent error handling for performance
                    }
                }
            });
        }
    }

    // THREAD-SAFE: This method runs on the consumer thread, so it's safe to call consumer.commitSync
    private void commitOffsetsOnConsumerThread() {
        try {
            synchronized (locker) {
                long offset = lastCommittedOffset.get();
                
                // Find consecutive completed offsets - EXACTLY like .NET algorithm
                while (completedOffsets.contains(offset + 1)) {
                    offset++;
                    completedOffsets.remove(offset);
                }
                
                if (offset > lastCommittedOffset.get()) {
                    lastCommittedOffset.set(offset);
                    
                    if (topicPartition != null) {
                        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                        commitMap.put(topicPartition, new OffsetAndMetadata(offset + 1));
                        consumer.commitSync(commitMap);
                    }
                }
            }
        } catch (Exception e) {
            // Silent error handling for performance
        }
    }

    public void shutdown() {
        if (!running) return;
        
        running = false;
        
        try {
            // Stop worker pool
            workerPool.shutdown();
            workerPool.awaitTermination(10, TimeUnit.SECONDS);
            
            consumer.close();
            
        } catch (Exception e) {
            // Silent error handling for performance
        }
    }

    private ConsumerRecord<Void, byte[]> createShutdownMarker() {
        return new ConsumerRecord<>(topicName, 0, -1, null, new byte[0]);
    }
}
