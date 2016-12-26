Geo distance calculator challenge
=================================

The geo calculation solution was created using Scala 2.11 and Redis for storing the geo data and calculating the closest points.
 On the resource folder there is a file "app.yml" which contains the application parameters such as:
 - Redis server configuration and the distance threshold in kilometers that will be used for searching the closest point.
 - Input folder: contains the list of inputs required for processing
 - Output folder: the result file for the processing, or list of closest points for the inputs
 - Data folder: application data, or airports

Requires:
- Java 8

Get Redis 3.2 from https://redis.io/download

Start redis:
./redis-server

Compile:

./gradlew jar

Execute:

java -jar build/libs/geochallenge-1.0-SNAPSHOT.jar