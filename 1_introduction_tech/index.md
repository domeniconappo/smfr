## Architecture notes

System is build based on <a href="https://www.reactivemanifesto.org/" target="_blank">Reactive Architecture paradigm.</a> 
We struggled to make the system resilient, allowing loose coupled components to rely on asynchronous message delivery (Message Driven).

The solution was conveniently developed as a set of containerized services, by adopting docker technologies (Docker, Docker Compose, Docker Swarm).

![A bird eye overview of the architecture]({{site.baseurl}}/media/smfr-architecture.png) 

It follows a detailed description of each component.

## Functional Components

These are components built up out of the SMFR Software Architecture definition.

### REST Server

The REST API Server works as a Facade component for the other services. 

It accepts requests to: 

- create, start and stop a twitter collection
- to list and filter all collections
- to manage and monitor twitter streamers

Full docs about exposed REST API are [here]({{site.baseurl}}/todo/)

The web component is a simple demo app that uses REST API Server to get and present data. 

### Persister

The Persister component was not present in the original Architecture definition. It's actually a technical component whose only aim is 
to handle storing tweets data storing in Cassandra.
It's connected as a consumer to the PERSISTER kafka queue. It decodes messages from the queue, building the tweet objects to save into DB.

It also expose a simple HTTP interface to retrieve counters (i.e. number of saved tweets since last restart). 
 
### Annotator

This is one of the core component of SMFR. At its bootstrap, it spawn different processes (one per each language defined) to load most recent machine learning models.
Each annotator process listens to the dedicated kafka queue (e.g. ANNOTATOR-DE for german tweets), decode the message from queue, classifiy the tweet and send it to the PERSISTER queue.

### Geocoder

Another fundamental component of the platform is the Geocoder. It listens to the GEOCODER queue and it tries to geolocalize each tweet based on full text.
A simple heuristic is applied in case of ambiguity or failures in geo-parsing. 

Once the tweet is geoparsed, it's sent out to the PERSISTER queue again so that it will be updated in DB. 

Furthermore, to proper aggregate results, the Geocoder will assign an EFAS region (i.e. a NUTS2 area) to the tweet.
  

### Aggregator

Aggregation is preparatory for the products. It will run periodically with running[^1] collections as input, and will count geocoded tweets per each collection/NUTS area.
It also collects tweets above a configured threshold and trends (i.e. percentage variation per day about relevant tweets) for each running collection.

[^1]: Aggregator is configurable to perform on (1) running, (2) background or (3) all collections. You can also pass a list of collection ids.


### Products

This component provides GeoJSON products, representing areas and risk grade, most relevant tweets per reported area and its trends per day. These files are disseminated to a list of servers[^2]

Products are built up from aggregated values, applying the following heuristics:

| Risk Color  | Description |
| ----------- | ----------- |
| Gray        | Less than 10 high relevant tweets|
| Orange      | 1/5 number of medium relevant tweets > number of high relevant tweets > 1/9 number of medium relevant tweets|
| Red         | number of high relevant tweets > 1/5 number of medium relevant tweets        |

This heuristic can be describedf by the string `10:5:9` which is configurable.

The 5[^3] most relevant tweets per area come from those having the highest probability in the relative collection, 
after deduplication and index scoring based on multiplicity and centrality. 

[^2]: Currently you can configure two SFTPs servers.
[^3]: This number is configurable
 
### Web

It's a simple dockerized web app built with Flask. It shows list of active collections, 
an interface to start and stop collections, an admin panel to show status of Twitter stremers and counters, and a page with most recent produced GeoJSON.

## Infrastructure Components

These components are databases, messaging systems and the like.
 
### DB MySQL

A RDBMS to store collections' parameters, aggregation results, and products. 
It also has tables for EFAS NUTS2 areas and NUTS3 associated to each NUT2. SMFR will use NUTS3 names as keywords to collect tweets from a NUTS2 area.
 
### DB Cassandra

A NoSQL database to store tweets data, very performant in writes. Currently, configuration allows one single node but it's intented to be used as a cluster.

### Kafka/Zookeeper

The messaging system of choice for SMFR. Kafka needs Zookeeper to operate so these are actually two dependant dockerized services.

### Geonames (ElasticSearch)

The Geocoder component queries an index stored on ES. This index is built out of Geonames.org gazetter. 

More details [here](https://github.com/openeventdata/es-geonames). 
