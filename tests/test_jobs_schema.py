"""Tests for context builder schema helpers."""

from MDWFutils.jobs.schema import ContextParam, _deduplicate_schema, common_wit_gpu_params


def test_deduplicate_schema_keeps_last_definition():
    schema = [
        ContextParam("nodes", int, default=1),
        ContextParam("nodes", int, default=4),
        ContextParam("queue", str, default="regular"),
    ]
    deduped = _deduplicate_schema(schema)
    assert len(deduped) == 2
    nodes = next(p for p in deduped if p.name == "nodes")
    assert nodes.default == 4


def test_common_wit_gpu_params_override_account():
    params = {p.name: p.default for p in common_wit_gpu_params() if p.default is not None}
    assert params["account"] == "m2986_g"
    assert params["constraint"] == "gpu"
