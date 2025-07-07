package com.kafkatps.kafkatps;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class DatabaseConnectionPoolProvider {
    
    private final HikariDataSource dataSource;
    private final AtomicLong processedCount = new AtomicLong(0);
    
    // Async executor for database operations - like .NET's Task.Run
    private final ExecutorService dbAsyncExecutor;
    
    // Pre-compiled SQL for maximum performance - exactly like .NET
    private static final String INSERT_LOAN_SQL = """
        INSERT INTO loans (
            id, member_id, amount, version, is_marked_to_delete,
            created_date, created_by, language, tenant_id, service_id,
            vertical_id, last_updated_date, last_updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
    
    public DatabaseConnectionPoolProvider() {
        this.dataSource = createConnectionPool();
        // Create dedicated thread pool for async DB operations - like .NET's ThreadPool
        this.dbAsyncExecutor = Executors.newFixedThreadPool(50, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("DbAsync-" + t.getId());
            return t;
        });
    }
    
    private HikariDataSource createConnectionPool() {
        HikariConfig config = new HikariConfig();
        
        // .NET-style connection string converted to JDBC
        config.setJdbcUrl("jdbc:postgresql://10.44.77.196:5432/microfinance_db");
        config.setUsername("postgres");
        config.setPassword("localpass123");
        config.setDriverClassName("org.postgresql.Driver");
        
        // ULTRA HIGH-PERFORMANCE pool settings for maximum TPS
        config.setMaximumPoolSize(200);  // Increased from 100 to 200
        config.setMinimumIdle(100);      // Increased from 50 to 100
        config.setConnectionTimeout(1000); // Reduced from 3000 to 1000ms
        config.setIdleTimeout(60000);    // Reduced from 300000 to 60000ms
        config.setMaxLifetime(600000);   // Reduced from 1800000 to 600000ms
        config.setLeakDetectionThreshold(30000); // Reduced from 60000 to 30000ms
        config.setValidationTimeout(1000); // Fast validation
        
        // Performance optimizations
        config.setAutoCommit(true);      // Auto-commit for single operations
        config.setReadOnly(false);
        // Fixed PostgreSQL configuration - remove invalid parameters
        config.setConnectionInitSql("SET synchronous_commit = OFF");
        
        // Connection pool optimizations
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "500"); // Increased from 250
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "4096"); // Increased from 2048
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        config.addDataSourceProperty("tcpKeepAlive", "true");
        config.addDataSourceProperty("socketTimeout", "5000");
        config.addDataSourceProperty("loginTimeout", "5");
        config.addDataSourceProperty("connectTimeout", "5");
        
        return new HikariDataSource(config);
    }
    
    // INDIVIDUAL ASYNC DATABASE OPERATION - exactly like .NET's async await pattern
    public CompletableFuture<Void> insertLoanAsync(UUID memberId, UUID tenantId, UUID verticalId, UUID userId, String language, String serviceId) {
        return CompletableFuture.runAsync(() -> {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(INSERT_LOAN_SQL)) {
                
                UUID loanId = UUID.randomUUID();
                long currentTimeMillis = System.currentTimeMillis();
                java.sql.Timestamp currentTime = new java.sql.Timestamp(currentTimeMillis);
                
                // Set parameters exactly like .NET CreateLoan method
                stmt.setObject(1, loanId);              // id
                stmt.setObject(2, memberId);            // member_id
                stmt.setDouble(3, 1.0);                 // amount
                stmt.setInt(4, 0);                      // version
                stmt.setBoolean(5, false);              // is_marked_to_delete
                stmt.setTimestamp(6, currentTime);      // created_date
                stmt.setObject(7, userId);              // created_by
                stmt.setString(8, language);            // language
                stmt.setObject(9, tenantId);            // tenant_id
                stmt.setString(10, serviceId);          // service_id
                stmt.setObject(11, verticalId);         // vertical_id
                stmt.setTimestamp(12, currentTime);     // last_updated_date
                stmt.setObject(13, userId);             // last_updated_by
                
                stmt.executeUpdate();
                
                // Performance monitoring without logging overhead
                processedCount.incrementAndGet();
                
            } catch (SQLException e) {
                throw new RuntimeException("Failed to insert loan", e);
            }
        }, dbAsyncExecutor);
    }
    
    public void close() {
        if (dbAsyncExecutor != null && !dbAsyncExecutor.isShutdown()) {
            dbAsyncExecutor.shutdown();
        }
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}