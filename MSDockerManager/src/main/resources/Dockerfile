FROM java:8
WORKDIR /
ADD MStormWorker.jar MStormWorker.jar
ADD RanSenStat.jar RanSenStat.jar
ADD log4j.properties log4j.properties
#ADD EdgeStorm EdgeStorm 
EXPOSE 8080
CMD java -jar MStormWorker.jar
