FROM openjdk:19
MAINTAINER software.ulpgc.bigData.InvertedIndex.DataMartBuilder
COPY target/DatamartBuilder-1.0-SNAPSHOT-jar-with-dependencies.jar datamart.jar
ENTRYPOINT ["java","-jar","datamart.jar"]