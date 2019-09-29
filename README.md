# kafka-template
Some notes about how to use Kafka with Node

##  Prerequisites
To run this Kafka configuration, *docker* with *docker-compose* is required.
The image for the [Apache Kafka](http://kafka.apache.org/) is available directly from [Docker Hub](https://hub.docker.com/r/wurstmeister/kafka/). For up-to-date documentation see [wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker).

## Usage
---
Export your environment before running the containers.
```
export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)
```
Why `HOST_IP` is required is explained in the *kafka-docker* connectivity [guide](https://github.com/wurstmeister/kafka-docker/wiki/Connectivity). To get the value of the `HOST_IP` use 
```
echo $HOST_IP
```
Run the containers.
```
docker-compose up
```
According to `KAFKA_CREATE_TOPICS` environment variable `topic-test` was automatically created. The topic has 1 partition and 1 replica.

### CLI
Basic operations with Kafka using CLI will be done through the docker container.

1. Connecting to the container.
    * List running containers on your machine.
        ```
        docker ps
        ```
    * Copy `CONTAINER ID` for the *wurstmeister/kafka* image.
    * Connect to the container
        ```
        docker exec -it <CONTAINER ID> sh
        ```
2. Create a topic. (Use new terminal window.)
    ```
    kafka-topics.sh --zookeeper zookeeper:2181 --topic first-topic --create --partitions 3 --replication-factor 1
    ```
    Using *docker-compose* it is possible to set the host of the *zookeeper* as `zookeeper:2181` but to connect from Node will be used `HOST_IP:2181`. The name of the topic is `first-topic`.
3. List all topics.
    ```
    kafka-topics.sh --zookeeper zookeeper:2181 --list
    ```
4. Get information about the topic.
    ```
    kafka-topics.sh --zookeeper zookeeper:2181 --topic first-topic --describe
    ```
5. Delete the topic.
    ```
    kafka-topics.sh --zookeeper zookeeper:2181 --topic first-topic --delete
    ```
6. List of all options for *kafka-topics*.
    ```
    kafka-topics.sh
    ```
7. Create a producer. (Use new terminal window.)
    ```
    kafka-console-producer.sh --broker-list kafka:9092 --topic first-topic --producer-property acks=all
    ```
    Kafka host was set as `kafka:9092` according to the *docker-compose*. From the Node use `HOST_IP:9092`. The producer will send messages to the *first-topic*. Create some messages from the console.
8. Create a consumer. (Use new terminal window.)
    ```
    kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic first-topic --from-beginning
    ```
    The consumer is reading messages from the *first-topic*.
9. Create a consumer with the consumer group. (Repeat the operation several times using different terminal windows.)
    ```
    kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic first-topic --group first-group
    ```
    Group name is *first-group*. Kafka will balance the distribution of the messages between the consumers of the group.
10. List the groups.
    ```
    kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list
    ```
11. Get information about the group.
    ```
    kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group first-group --describe
    ```
    `CURRENT-OFFSET`,  `LOG-END-OFFSET`, and  `LAG` for each partition will be shown as well as `CONSUMER-ID`.
