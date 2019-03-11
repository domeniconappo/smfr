## Architecture notes

System is build based on <a href="https://www.reactivemanifesto.org/" target="_blank">Reactive Architecture paradigm.</a> 
We struggled to make the system resilient, allowing loose coupled components to rely on asynchronous message delivery (Message Driven).

The solution was conveniently developed as a set of containerized services, by adopting docker technologies (Docker, Docker Compose, Docker Swarm).

![A bird eye overview of the architecture]({{site.baseurl}}/media/smfr-architecture.png) 

It follows description of technology stack for each component.

## Functional Components

### REST Server

### Persister

### Annotator

### Geocoder

### Aggregator

### Products

## Infrastructure Components

### DB MySQL

### DB Cassandra

### Kafka

### Geonames (ElasticSearch)
