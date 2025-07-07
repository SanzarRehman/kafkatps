package com.kafkatps.kafkatps;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.CountDownLatch;

public class Main {
    
    private static final CountDownLatch latch = new CountDownLatch(1);
    
    public static void main(String[] args) throws InterruptedException {
        String kafkaServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "10.42.53.125:19092,10.42.53.125:29092,10.42.53.125:39092");
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-k":
                    if (i + 1 < args.length) {
                        kafkaServers = args[++i];
                    }
                    break;
                case "-p":
                    if (i + 1 < args.length) {
                   //     postgreSQLServer = args[++i];
                    }
                    break;
            }
        }

        

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(R2dbcConfig.class);
        context.refresh();


        DatabaseConnectionPoolProvider databaseProvider = context.getBean(DatabaseConnectionPoolProvider.class);

        KafkaMessageConsumerService kafkaConsumerService = new KafkaMessageConsumerService(
                "Microfinance.Commands.DisburseCommand",   // SAME topic name as .NET
                Integer.valueOf(System.getenv().getOrDefault("KAFKA_CONCURRENCY", "50")),                      // INCREASED concurrency from 50 to 100 threads
                kafkaServers,              // Kafka bootstrap servers
                System.getenv().getOrDefault("KAFKA_GROUP", "simple"),                  // SAME group ID as .NET
                databaseProvider           // High-performance database provider
        );

        kafkaConsumerService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumerService.stop();
            context.close();
            latch.countDown();
        }));
        
        latch.await();
    }
}
