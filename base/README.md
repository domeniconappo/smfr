# SMFR Docker base image

Docker image to use as base for SMFR containers.
The image is based on python:3.6 official Docker image and it's got cassandra-driver, numpy, tensorflow, kafka-python pre-installed.
Its purpose is to reduce build time for images sharing heavy packages (e.g. avoiding to download and rebuild the same heavy package at each image rebuild).

In microservices Dockerfile put this as first line:

```
FROM efas/smfr_base:latest
```
