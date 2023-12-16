import os
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException

COUCHBASE_URL = os.getenv('COUCHBASE_URL', 'couchbase://couchbase-server')
COUCHBASE_BUCKET = os.getenv('COUCHBASE_BUCKET', 'dataIngestionBucket')
COUCHBASE_SCOPE = os.getenv('COUCHBASE_SCOPE', 'myScope')
COUCHBASE_COLLECTION = os.getenv('COUCHBASE_COLLECTION', 'counters')
COUCHBASE_USERNAME = os.getenv('COUCHBASE_USERNAME', 'user')
COUCHBASE_PASSWORD = os.getenv('COUCHBASE_PASSWORD', 'password')

# Connect to Couchbase
authenticator = PasswordAuthenticator(COUCHBASE_USERNAME, COUCHBASE_PASSWORD)
cluster = Cluster(COUCHBASE_URL, ClusterOptions(authenticator))
bucket = cluster.bucket(COUCHBASE_BUCKET)
scope = bucket.scope(COUCHBASE_SCOPE)
collection = scope.collection(COUCHBASE_COLLECTION)
# Counter document key in Couchbase
counter_key = "instance-counter"


# Function to get a unique sequential ID
def get_sequential_id():
    try:
        # Increment and retrieve the counter value
        counter_result = collection.binary().increment(counter_key)
        return counter_result.content
    except DocumentNotFoundException:
        collection.upsert(counter_key, {"value": 0})
        return 0
    except CouchbaseException as e:
        exit(1)


# Get a unique sequential ID for this instance
instance_id = get_sequential_id()
print(instance_id)

