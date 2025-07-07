# ---------- BUILD STAGE ----------
FROM eclipse-temurin:21-jdk-ubi9-minimal AS builder

WORKDIR /workspace

# Install tools needed for Gradle (xargs, etc.)
RUN microdnf install -y findutils

# Copy Gradle files first for Docker layer caching
COPY build.gradle settings.gradle gradlew /workspace/
COPY gradle /workspace/gradle

# Download dependencies only
RUN ./gradlew --no-daemon build -x test

# Copy the full project source after dependencies are cached
COPY src /workspace/src

# Build the fat jar
RUN ./gradlew --no-daemon clean jar

# ---------- RUNTIME STAGE ----------
FROM eclipse-temurin:21-jre-ubi9-minimal

WORKDIR /app

# Copy only the built jar from the builder stage
COPY --from=builder /workspace/build/libs/*.jar app.jar

# Run with recommended options for modern JVMs
ENTRYPOINT ["java", "--enable-preview", "-XX:+ExitOnOutOfMemoryError", "-jar", "app.jar"]
