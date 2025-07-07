package com.kafkatps.kafkatps;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class R2dbcConfig {

    @Bean
    public DatabaseConnectionPoolProvider databaseConnectionPoolProvider() {
        return new DatabaseConnectionPoolProvider();
    }
}