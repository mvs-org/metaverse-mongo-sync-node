# Metaverse MongoDB Sync
Docker image to sync from Metaverse mvsd to MongoDB.

[![Build Status](https://travis-ci.org/canguruhh/metaverse-mongo-sync-node.png?branch=master)](https://travis-ci.org/canguruhh/metaverse-mongo-sync-node)

# Run
You can build the image or use the image form docker hub.
``` bash
docker run cangr/mvsd-mongo-sync
```

# Setup
To configure the sync service you need to set the environment variables:
- MONGO_HOST
- MONGO_PORT
- MONGO_DB
- MVSD_HOST
- MVSD_PORT
