from redis import Redis


class NamespaceClient:
    def __init__(self, client: Redis):
        self.client = client

    def get_client(self, namespace: str) -> Redis:
        return self.client.namespace(namespace)
