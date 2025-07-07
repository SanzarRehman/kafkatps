package com.kafkatps.kafkatps;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.CountDownLatch;

public class Main {
    
    private static final CountDownLatch latch = new CountDownLatch(1);
    
    public static void main(String[] args) throws InterruptedException {
        // Parse command line arguments like .NET version
        String kafkaServers = "localhost:9092";
        String postgreSQLServer = "10.44.77.196-5432-postgres-localpass123";
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-k":
                    if (i + 1 < args.length) {
                        kafkaServers = args[++i];
                    }
                    break;
                case "-p":
                    if (i + 1 < args.length) {
                        postgreSQLServer = args[++i];
                    }
                    break;
            }
        }
        
        if (kafkaServers.isEmpty() || postgreSQLServer.isEmpty()) {
            System.out.println("Usage: java -jar app.jar -k <kafka_servers> -p <postgresql_server>");
            System.out.println("PostgreSQL server format: <host>-<port>-<username>-<password>");
            return;
        }
        
        String[] postgreSqlParams = postgreSQLServer.split("-");
        if (postgreSqlParams.length != 4) {
            System.out.println("PostgreSQL server parameters should be in format: <host>-<port>-<username>-<password>");
            return;
        }
        
        // Create Spring context for dependency injection
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(R2dbcConfig.class);
        context.refresh();

        // Create high-performance database provider
        DatabaseConnectionPoolProvider databaseProvider = context.getBean(DatabaseConnectionPoolProvider.class);
        
        // Configure exactly like .NET version with same parameters
        KafkaMessageConsumerService kafkaConsumerService = new KafkaMessageConsumerService(
                "disburse-commands-test",   // SAME topic name as .NET
                200,                        // SAME concurrency as .NET (50 threads)
                kafkaServers,              // Kafka bootstrap servers
                "simple",                  // SAME group ID as .NET
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
