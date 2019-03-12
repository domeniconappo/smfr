## Introduction

Social Media Flood Risk (SMFR) is a platform to monitor specific flood events on social media (currently, only Twitter).

The software is developed as Exploratory Research project at the JRC - European Commission facilities and is released under [EUPL licence](LICENCE).

Current version is based on:
  - Twitter Stream API to filter tweets using keywords and/or bounding box criteria
  - ML algorithms and models for classification of tweets (flood relevance)
  - Geonames index for geocoding of relevant tweets
  - Cassandra to keep raw tweets data
  - Kafka and MySQL as infrastructure elements

When a potential catastrofic flood event is recorded in EFAS, SMFR is notified and will start to:

  - Collect tweets with Twitter Stream API
  - A pipeline is implemented to collect, annotate and geocode tweets in sequence.

Final product of SMFR is an event-related map reporting relevant tweets and affected areas.


## Main SMFR Concepts

The system was built around some concepts and assumptions and it's intented to work as a complementary monitoring service for existing early risk alert systems.
The first release of this experimental project is tailored to work with [EFAS](http://www.efas.eu) but in future releases the "topic" (e.g. floods, forest fires...) and the "main" alert system might be configurable.

A list of specific terms that will be used extensively in this documentation is reported below:

- (Twitter) Collection

A collection is an entity representing an istance of a Twitter Stream API connection (or a contributing part to it). 
It's stored in a SQL DB (i.e. MySQL) and it holds all information that a streamer needs to operate:
  - Keywords
  - Bounding Box
  - Runtime (i.e. "run until" time)

- Trigger

It's a Collection attribute and indicates whatever the collection was triggered by an EFAS event (_on-demand_ trigger), or it was manually inserted (e.g. because of a flash flood, _manual_ trigger), or if it's simply a background collection.
The background collection is a special case of collection, running constantly and without any specified bounding box.
   
- Streamer

The streamer is a process that connects to Twitter Stream API and defines keywords and bounding boxes. In SMFR thare are three different streamers, one for each kind of trigger.
This means you need three different [Twitter apps accounts](https://developer.twitter.com/en/apps), with token and consumer keys and secrets to use to connect. 

**_Remember you can only connect one streamer per each Twitter App account._** 

- NUTS area

It's an administrative region based on European Union code, with a different level of granularity. In EFAS and SMFR, european regions are at NUTS2 level.
A geocoded tweet will have the assigned NUTS2 code (for on-demand collections, if the tweet is geocoded outside the bounding box of the collection, it will be discarded).

- TBD...
