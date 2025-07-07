# Use Eclipse Temurin Java 24 JDK on Ubuntu Jammy
FROM eclipse-temurin:24.0.1_9-jdk-ubi9-minimal

# Create a non-root user for security (optional but recommended)
RUN useradd -m appuser

# Set working directory
WORKDIR /app

# Copy the fat jar built outside Docker
COPY kafkatps-0.0.1-SNAPSHOT.jar app.jar

# Fix ownership so appuser can access files
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# JVM options:
# -XX:+UseContainerSupport to respect container limits
# -XX:+ExitOnOutOfMemoryError for safer crashes
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:+ExitOnOutOfMemoryError", "-jar", "app.jar"]
