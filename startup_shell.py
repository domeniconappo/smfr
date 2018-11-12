from smfrcore.models.sql import *
from smfrcore.models.cassandra import *
app=create_app()
app.app_context().push()
