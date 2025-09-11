# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Architecture

This is a Spring Boot application with Kafka integration and KEDA autoscaling capabilities, designed for high-throughput message processing with automatic consumer scaling.

### Key Technologies
- **Spring Boot 3.5.5** with Azul Zulu JDK 24 toolchain
- **Apache Kafka 3.9** for message streaming
- **Kubernetes KEDA (HPA)** for consumer lag-based autoscaling
- **Spring Modulith** for modular architecture with event-driven communication
- **Lombok** for reducing boilerplate code
- **Gradle** as the build system

### Architecture Overview
- **Partition Strategy**: Uses Kafka partitioning for concurrency guarantees
- **Auto-scaling**: KEDA monitors consumer lag and scales when lag > 1000 messages
- **Message Size**: Handles 2-3KB JSON payloads per message
- **Modular Design**: Spring Modulith for organizing code into event-driven modules
- **Container Orchestration**: Kubernetes-based consumer scaling

### Sample Request Format
```json
{
  "userId": "user-A",
  "orderId": "order-101", 
  "productName": "Laptop",
  "eventTimestamp": "2023-10-27T10:00:00Z"
  // ... additional fields
}
```

## Common Commands

### Build & Run
```bash
./gradlew build                 # Build the project
./gradlew bootRun              # Run the Spring Boot application
./gradlew clean                # Clean build artifacts
```

### Testing
```bash
./gradlew test                 # Run all tests
./gradlew test --tests <TestClass>  # Run specific test class
```

### Development
```bash
docker-compose up -d            # Start Kafka (KRaft mode) and Kafka UI
./gradlew bootRun              # Run the application
```

### Docker & Kubernetes
```bash
docker build -t kafka-keda-skeleton .   # Build Docker image
kubectl apply -f k8s/            # Deploy to Kubernetes with KEDA scaling
```

The project includes Spring Boot DevTools for hot reloading during development.

## Package Structure
- Main application: `src/main/java/org/example/kafka/`
- Application entry point: `KafkaSkeletonApplication.java`
- Configuration: `src/main/resources/application.properties`
- Tests: `src/test/java/org/example/kafka/`

## Dependencies Notes
- Spring Modulith BOM version 1.4.1 manages modular dependencies
- Kafka events are handled through `spring-modulith-events-kafka` runtime dependency
- Docker Compose integration available for development environment setup