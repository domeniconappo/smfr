# Backupper container

## Usage

```bash
$ docker run -v smfr_vlm-mysql:/volume -v /tmp:/backup --rm backupper backup some_archive
$ docker run -v smfr_vlm-cassandra:/volume -v /tmp:/backup --rm backupper backup some_archive
```

or in Swarm mode, just change volumes' prefix to SMFR:

```bash
$ docker run -v SMFR_vlm-mysql:/volume -v /tmp:/backup --rm backupper backup some_archive
$ docker run -v SMFR_vlm-cassandra:/volume -v /tmp:/backup --rm backupper backup some_archive
```
