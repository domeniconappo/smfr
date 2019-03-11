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
