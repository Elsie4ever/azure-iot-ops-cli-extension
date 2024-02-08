# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import pytest

from azext_edge.edge.common import ResourceTypeMapping
from azext_edge.edge.commands_datasets import query_datasets

from .conftest import DATASETS_PATH, generate_keys
from .....generators import generate_generic_id


@pytest.mark.parametrize("mocked_build_query", [{
    "path": DATASETS_PATH,
    "result": [{"result": generate_generic_id()}]
}], ids=["query"], indirect=True)
@pytest.mark.parametrize("req", [
    {},
    {
        "keys": '{\"key1\": {\"path\": \".\",\"primaryKey\": true}}',
    },
    {
        "dataset_name": generate_generic_id(),
        "description": generate_generic_id(),
        "payload": generate_generic_id(),
        "custom_location_name": generate_generic_id(),
        "timestamp": generate_generic_id(),
        "ttl": generate_generic_id(),
        "keys": '{\"key1\": {\"path\": \".\",\"primaryKey\": true}}',
    },
])
def test_query_datasets(mocked_cmd, mocked_get_subscription_id, mocked_build_query, req):
    result = query_datasets(
        cmd=mocked_cmd,
        **req
    )
    assert result == mocked_build_query.return_value
    query_args = mocked_build_query.call_args.kwargs
    assert query_args["subscription_id"] == mocked_get_subscription_id.return_value
    assert query_args["location"] == req.get("location")
    assert query_args["resource_group"] == req.get("resource_group_name")
    assert query_args["type"] == f"{ResourceTypeMapping.instance.value}/datasets"
    assert query_args["additional_project"] == "extendedLocation"

    expected_query = []
    if req.get("description"):
        expected_query.append(f" where properties.description =~ \"{req['description']}\"")
    if req.get("payload"):
        expected_query.append(f" where properties.payload =~ \"{req['payload']}\"")
    if req.get("timestamp"):
        expected_query.append(f" where properties.timestamp =~ \"{req['timestamp']}\"")
    if req.get("ttl"):
        expected_query.append(f" where properties.ttl =~ \"{req['ttl']}\"")
    if req.get("keys"):
        expected_query.append(" where properties.keys.key1.path =~ \".\"")
        expected_query.append(" where properties.keys.key1.primaryKey =~ \"True\"")
    if req.get("custom_location_name"):
        expected_query.append(f" where extendedLocation.name contains \"{req['custom_location_name']}\"")
    if req.get("dataset_name"):
        expected_query.append(f" where name =~ \"{req['dataset_name']}\"")

    custom_query = query_args["custom_query"].split("|")[1:]

    assert len(custom_query) == len(expected_query)
    for i in expected_query:
        assert i in custom_query
