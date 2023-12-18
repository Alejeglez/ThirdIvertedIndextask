FROM openjdk:19
MAINTAINER software.ulpgc.bigData.InvertedIndex.BookApi
COPY target/book-api-1.0-SNAPSHOT-jar-with-dependencies.jar api.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/api.jar"]