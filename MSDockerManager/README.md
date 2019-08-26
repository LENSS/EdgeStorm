Docker Manager for EdgeStorm

install docker in Ubuntu 16.04:

    chmod +x installDocker.sh
    sudo ./installdocker.sh

Build and run:
    
    mvn clean package

    java -jar target/docker-http-client-0.0.1-SNAPSHOT.jar 

Support memory/CPU limits:
https://www.serverlab.ca/tutorials/containers/docker/how-to-limit-memory-and-cpu-for-docker-containers/
 
