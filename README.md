# Table of Contents
    1. Infrastructure
        1.1. Dependencies
            1.1.1. InfluxDB
            1.1.2. Zookeeper
            1.1.3. Kafka
            1.1.4. Grafana
        1.2. Deployment
            1.2.1. Switch to the swarm mode
            1.2.2. Create a network
            1.2.3. Run Zookeeper
            1.2.4. Run Kafka
            1.2.6. Run Grafana
            1.2.7. Run Influxdb
    2. Configuration
        2.1. Sections description
            2.1.1. Section "input"
            2.1.2. Section "output"
            2.1.3. Section "processing"
            2.1.4. Section "databases"
            2.1.5. Section "analysis"
    3. List of fields for transformations
    4. Running application

## Infrastructure
Before starting the application, you need to deploy the following services:

* Kafka + Zookeeper
* Grafana
* InfluxDB

### Dependencies
To download necessary docker images, run the following commands:

* InfluxDB: `docker pull influxdb`
* Zookeeper: `docker pull confluentinc/cp-zookeeper:3.0.0`
* Kafka: `docker pull confluentinc/cp-kafka:3.0.0`
* Grafana: `docker pull grafana/grafana`

### Deployment
* Switch to the swarm mode: 

	```bash
	docker swarm init
	```

* Create an overlay network: 
	
	```bash
	docker network create --driver overlay --attachable=true network-name
	```
* Run Zookeeper: 

	```bash
	docker service create --name=zookeeper --mount type=bind,source=/path/to/folder,destination=/var/lib/zookeeper/data \
		-e ZOOKEEPER_CLIENT_PORT=32181 -e ZOOKEEPER_TICK_TIME=2000 --network=network-name confluentinc/cp-zookeeper:3.0.0
	```

* Run Kafka: 
	
	```bash
	docker service create --name=kafka --mount type=bind,source=/path/to/folder,destination=/var/lib/kafka/data \
		-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:32181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 \
		--network=network-name confluentinc/cp-kafka:3.0.0
	```

* Run Grafana: 
	
	```bash
	docker service create --name=view -p 3000:3000 --env GF_SECURITY_ADMIN_PASSWORD=your_password \
		--network=network-name grafana/grafana
	```
    
        login: admin, password: your_password
    
* Run InfluxDB: 
	
	```bash
	docker service create --name=influxdb --mount type=bind,source=/path/to/folder,destination=/var/lib/influxdb \
		--network=network-name influxdb
	```
    
        login: root, password: root

## Configuration

To run the application, you will need a configuration file. It is a json file with the following structure:

```json
{
  "input": {
    "input_type": "kafka",
    "data_structure": "config_data_structure.json",
    "options": {
      "server": "zookeeper",
      "port": 32181,
      "consumer_group": "data-consumer",
      "topic": "sensors-demo",
      "batchDuration": 10,
      "sep": ","
    }
  },
  "outputs": [{
    "main": true,
    "method": "influx",
    "options": {
      "influx": {
        "host": "influxdb",
        "port": 8086,
        "username": "root",
        "password": "root",
        "database": "sensors",
        "measurement": "points"
      }
    }
  }],
  "processing": {
    "transformation": [
    "counter: one(timestamp)",
    "sensor_id",
    "sensor_type",
    "rpm_min: rotation_speed",
    "rpm_max: rotation_speed",
    "rpm_sum: rotation_speed",
    "speed_lt: lt(rotation_speed, 1000)",
    "speed_gt: gt(rotation_speed, 4000)"
    ],
    "aggregations": {
      "operation_type": "reduceByKey",
      "rule": [
    "key: (sensor_id, sensor_type)",
    "max(rpm_max)",
    "min(rpm_min)",
    "sum(rpm_sum)",
    "sum(speed_lt)",
    "sum(speed_gt)",
    "sum(counter)"
      ]
    }
  },
  "databases": {
  }
}
```
File has 5 sections: 4 of them are mandatory (input, output, processing, databases) and 1 is optional (analysis). Sections are described in more detail below (all fields are mandatory).

### Input Section
This section describes how the application will receive data.

* "input_type" - input data type, valid values: "csv", "kafka"
* "data_structure" - path to file with data structure 
* "options":
    * "server" - zookeeper hostname
    * "port" - zookeeper port
    * "consumer_group" - kafka consumer group
    * "topic" - kafka topic
    * "batchDuration" - data sampling window in seconds
    * "sep" - fields delimiter for received string

### Output Section
This section describes how the application will output data.

* "method" - data output type, valid values: "csv", "influx"
* "options" > influx":
    * "host" - influxdb hostname
    * "port": influxdb port
    * "username": influxdb user
    * "password": password for user above
    * "database": influxdb database name
    * "measurement": measurement name

### Processing Section
This section specifies transformations and aggregations to be performed on the input data.

* "transformation" - this array of strings specifies transformation steps for every string received from the input source.

   User can rename the field (new_name: old_name), apply one of defined functions: 
   sum, div, mul, sub, etc or just use the field unchanged.

   Each field declared in the transformation section should be subsequently used in aggregation, otherwise the application will raise exception.
   
* aggregation - this section specifies how the data should be aggregated after transformation.
    
  Field "operation_type" specifies aggregation type, valid values are "reduce" and "reduceByKey". In case of "reduceByKey" user needs to specify an aggregation key (key = src_country).
  
  The key can contain more than one field (key = (src_ip, dst_country, src_port_or_icmp_type)).
  
  User can specify one of defined aggregation functions: sum, mul, max, min.

### Databases Section
This section specifies paths to databases which are necessary for the udf functions to work.

### Analysis Section
This section specifies rules for data analysis and ways to notify about detected anomalies.

* Section "historical" is mandatory at the moment. It specifies that analysis will be based on historical data.
    * "method" - source of historical data, valid values: "influx"
    * "influx_options" - see section output > options
    
* Section "alert" specifies settings for notifications of detected anomalies.
    * "method" -specifies output method for notifications, valid values: "stdout", "kafka"
    * "options"
        * "server" - kafka hostname
        * "port" - kafka port
        * "topic" - kafka topic

* accuracy - accuracy in seconds, [ now - time_delta - accuracy; now - time_delta + accuracy ]

* Section "rule" contains an array of user-defined analysis modules with their respective names and options. System automatically imports class "SimpleAnalysis", so you don’t need to explicitly specify it.
    * "module" - name of the class to be used for analysis. Specified class should be located in a folder with the same name and needs to implement the IUserAnalysis interface. Method with name "analysis" should be implemented. This method will receive two arguments. First argument is an object which provides historical data access by index and field name. Second argument is an object which allows to send notifications by calling its method "send_message"
    * "name" - module name to be used in warning messages
    * "options" - settings to be passed to the class constructor. These are user defined and allow control over analysis behaviour

## Running application
When the infrastructure is deployed and the configuration file is ready, you can run the application running next steps. 

Build a base image for spark: 

```bash
docker build -t bw-sw-spark base-docker-image/
```

Run system tests to ensure everything is ok.

```bash
docker run --rm -v $(pwd):/project bw-sw-spark bash -c 'cd project && nosetests'
```

Build an image for the application: 

```bash
docker build -t processor-app .
```

Create kafka topic: 

```bash
docker run --network=network-name --rm confluentinc/cp-kafka:3.0.0 kafka-topics --create \
	--topic sensors-demo --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```

Run the application: 

```bash

docker service create --name=app --mount type=bind,source=$(pwd),destination=/configs \
	--network=network-name processor-app /configs/config_reducebykeys.json
```

Run sample data generator: 

```bash

python3 generator.py | docker run --network=network-name -i --rm confluentinc/cp-kafka:3.0.0 kafka-console-producer \
	--broker-list kafka:29092 --topic sensors-demo
```

## Maintain influxdb

You may need to drop series from influx or recreate new structure for data after changing configuration, use influxdb console for doing that.

```
docker run -it --rm --network=networks-name influxdb influx -host influxdb
```
