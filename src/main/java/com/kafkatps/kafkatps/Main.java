package com.kafkatps.kafkatps;

import com.kafkatps.kafkatps.repo.LoanRepository;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.CountDownLatch;

public class Main {
    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(R2dbcConfig.class);
        context.refresh();

        LoanRepository loanRepository = context.getBean(LoanRepository.class);
        // Configure Kafka consumer service
        KafkaMessageConsumerService kafkaConsumerService = new KafkaMessageConsumerService(
                "disburse-commands-test",   // topic name
                50,                    // concurrency
                "localhost:9092",      // bootstrap servers
                "my-consumer-group-test"
                ,loanRepository// group ID
        );

        kafkaConsumerService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            kafkaConsumerService.stop();
            context.close();
            latch.countDown();
        }));

        System.out.println("App running. Press Ctrl+C to exit.");
        latch.await();
    }
}
