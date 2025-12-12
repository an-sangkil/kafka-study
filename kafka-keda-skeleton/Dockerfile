#FROM azul/zulu-openjdk:24-jre-latest
FROM azul/zulu-openjdk:24-jdk-crac-latest


# 작업 디렉토리 설정
WORKDIR /app

# Gradle wrapper와 소스 코드 복사
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .
COPY src src

# 실행 권한 부여
RUN chmod +x gradlew

# 애플리케이션 빌드
RUN ./gradlew bootJar --no-daemon

# JAR 파일을 복사
RUN cp build/libs/*.jar app.jar

# 포트 노출
EXPOSE 8080

# 애플리케이션 실행
ENTRYPOINT ["java", "-jar", "app.jar"]