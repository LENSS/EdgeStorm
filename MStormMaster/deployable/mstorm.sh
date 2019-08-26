#! /bin/sh

# first change pwd to the current directory where this script is stored.
cd "$(dirname "$0")"

JARFILENAME="MStorm.jar"

# Carry out specific functions when asked to by the system
case "$1" in
  start)
    echo "Starting Zookeeper"
    zookeeper-3.4.6/bin/zkServer.sh start
    echo "Starting MStorm"
    java -jar $JARFILENAME localhost:2181 >> mstorm_terminal.out &
    ;;
  stop)
    echo "Stopping Zookeeper"
    zookeeper-3.4.6/bin/zkServer.sh stop
    echo "Stopping MStorm"
    kill $(ps -ef | grep $JARFILENAME | awk '{print $2}')
    ;;
  *)
    echo "Usage: mstorm {start|stop}"
    exit 1
    ;;
esac

exit 0


