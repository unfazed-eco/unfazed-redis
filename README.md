Unfazed Redis
====

Unfazed-Redis borned to solve namespace issues in Redis.


## Installation

```bash
pip install unfazed-redis
```


## Quick Start


### Add settings to your project


```python

# settings.py

UNFAZED_SETTINGS = {
    "CACHE": {
        "default": {
            "BACKEND": "unfazed_redis.backends.namespaceclient.NamespaceClient",
            "LOCATION": "redis://localhost:6379",
            "OPTIONS": {
                "PREFIX": "my_prefix",
                "VERSION": "1",
            }
        }
    }
}

```


### Use in your project


```python

# services.py

from unfazed.cache import caches

async def my_service():

    cache = caches["default"]
    await cache.set("my_key", "my_value")
    value = await cache.get("my_key")
    print(value)

```

