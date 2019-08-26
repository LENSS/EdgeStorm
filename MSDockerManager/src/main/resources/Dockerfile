FROM java:8
WORKDIR /
ADD MStormWorker.jar MStormWorker.jar
ADD RanSenStat.jar RanSenStat.jar
ADD EdgeStorm EdgeStorm 
EXPOSE 8080
CMD /EdgeStorm/mstorm.sh start ; sleep 5 ; java -jar MStormWorker.jar localhost
