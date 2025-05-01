@echo off
start java -Dserver.port=8080 -Dserver.node.value=1 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar
start java -Dserver.port=8081 -Dserver.node.value=2 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar
start java -Dserver.port=8082 -Dserver.node.value=3 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar
start java -Dserver.port=8083 -Dserver.node.value=4 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar
start java -Dserver.port=8084 -Dserver.node.value=5 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar
start java -Dserver.port=8085 -Dserver.node.value=6 -jar target/word-count-distributed-system-0.0.1-SNAPSHOT.jar