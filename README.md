# Project IBEX
A Celery application to collect data, download media and extract information from social media APIs.

## Requirements
You must have a Redis DB instance running (which is currently used as the Celery broker). 

Also, another DB will probably be used as the Celery backend in the future 
(if we ever need backend, that is).

You must also have a MongoDB instance running as the application's write layer.

## Usage

1. Run celery:
```
celery -A app.core.celery.worker worker -l info
```
Note, that Celery does not support **Windows**, so you should probably include
```
--pool=solo
```
to avoid any unnecessary errors (for testing only).

2. Run python server:
```
python server.py
```
And make sure to include **CROWDTANGLE_TOKEN** and **YOUTUBE_TOKEN** 
as environment variables and set credentials into app/core/datasources/twitter/.twitter_keys.yaml 
file for the respective collectors to work properly.
