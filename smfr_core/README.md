# SMFR Core Module

This module will be installed as a standard python package
in containers that needs SMFR core models and tools.

- web
- restserver
- annotator
- geocoder

Example of usage:

```python
from smfrcore.models.sqlmodels import TwitterCollection
tweets = Tweet.get_iterator(collection_id=0, ttype='collected')
```