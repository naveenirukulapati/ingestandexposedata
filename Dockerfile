FROM openjdk:8-jdk-alpine
VOLUME /tmp
COPY target/IngestAndExposeData-0.0.1-SNAPSHOT.jar IngestAndExposeData-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["/usr/bin/java"]
CMD ["-jar","/IngestAndExposeData-0.0.1-SNAPSHOT.jar"]
EXPOSE 8888