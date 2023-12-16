import os
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException, DocumentExistsException

# Couchbase configuration
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
def get_total_instances(collection):
    try:
        # Retrieve the current counter value without incrementing
        result = collection.get(counter_key)
        counter_value = result.content_as[int]  # Assuming the counter is stored as an integer
        return counter_value + 1
    except DocumentNotFoundException:
        # Initialize the counter if it does not exist
        collection = bucket.default_collection()
    except CouchbaseException as e:
        exit(1)

# Get a unique sequential ID for this instance
instance_id = get_total_instances(collection)
print(instance_id)