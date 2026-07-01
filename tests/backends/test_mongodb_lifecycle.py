"""MongoDB backend lifecycle behavior."""

from __future__ import annotations

from MDWFutils.backends import mongodb


class FakeCollection:
    def __init__(self):
        self.index_calls = []

    def create_index(self, *args, **kwargs):
        self.index_calls.append((args, kwargs))


class FakeDatabase:
    def __init__(self):
        self.ensembles = FakeCollection()
        self.operations = FakeCollection()
        self.measurements = FakeCollection()
        self.ensemble_defaults = FakeCollection()


class FakeMongoClient:
    instances = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.db = FakeDatabase()
        self.server_info_calls = 0
        self.close_calls = 0
        self.__class__.instances.append(self)

    def server_info(self):
        self.server_info_calls += 1
        return {"ok": 1}

    def get_database(self):
        return self.db

    def close(self):
        self.close_calls += 1


def test_mongodb_backend_can_skip_startup_validation_and_indexes(monkeypatch):
    FakeMongoClient.instances = []
    monkeypatch.setattr(mongodb, "MongoClient", FakeMongoClient)

    backend = mongodb.MongoDBBackend(
        "mongodb://host/db",
        validate_connection=False,
        ensure_indexes=False,
    )

    client = FakeMongoClient.instances[0]
    assert client.server_info_calls == 0
    assert client.db.ensembles.index_calls == []
    assert client.db.operations.index_calls == []
    assert client.db.measurements.index_calls == []
    assert client.db.ensemble_defaults.index_calls == []

    backend.validate_connection()
    backend.ensure_indexes()
    backend.ensure_indexes()

    assert client.server_info_calls == 1
    assert len(client.db.ensembles.index_calls) == 3
    assert len(client.db.operations.index_calls) == 4
    assert len(client.db.measurements.index_calls) == 1
    assert len(client.db.ensemble_defaults.index_calls) == 1

    backend.close()
    backend.close()

    assert client.close_calls == 1


def test_mongodb_backend_can_eagerly_validate_and_create_indexes(monkeypatch):
    FakeMongoClient.instances = []
    monkeypatch.setattr(mongodb, "MongoClient", FakeMongoClient)

    mongodb.MongoDBBackend(
        "mongodb://host/db",
        validate_connection=True,
        ensure_indexes=True,
    )

    client = FakeMongoClient.instances[0]
    assert client.server_info_calls == 1
    assert len(client.db.ensembles.index_calls) == 3
    assert len(client.db.operations.index_calls) == 4
    assert len(client.db.measurements.index_calls) == 1
    assert len(client.db.ensemble_defaults.index_calls) == 1
