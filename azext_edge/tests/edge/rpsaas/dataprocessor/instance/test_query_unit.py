# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import pytest

from azext_edge.edge.common import ResourceTypeMapping
from azext_edge.edge.commands_instances import query_instances

from .conftest import INSTANCES_PATH
from .....generators import generate_generic_id


@pytest.mark.parametrize("mocked_build_query", [{
    "path": INSTANCES_PATH,
    "result": [{"result": generate_generic_id()}]
}], ids=["query"], indirect=True)
@pytest.mark.parametrize("req", [
    {},
    {
        "instance_name": generate_generic_id(),
    },
    {
        "custom_location_name": generate_generic_id(),
        "description": generate_generic_id(),
    },
])
def test_query_datasets(mocked_cmd, mocked_get_subscription_id, mocked_build_query, req):
    result = query_instances(
        cmd=mocked_cmd,
        **req
    )
    assert result == mocked_build_query.return_value
    query_args = mocked_build_query.call_args.kwargs
    assert query_args["subscription_id"] == mocked_get_subscription_id.return_value
    assert query_args["location"] == req.get("location")
    assert query_args["resource_group"] == req.get("resource_group_name")
    assert query_args["type"] == ResourceTypeMapping.instance.value
    assert query_args["additional_project"] == "extendedLocation"

    expected_query = []
    if req.get("description"):
        expected_query.append(f" where properties.description =~ \"{req['description']}\"")
    if req.get("instance_name"):
        expected_query.append(f" where name contains \'{req['instance_name']}\'")
    if req.get("custom_location_name"):
        expected_query.append(f" where extendedLocation.name contains \"{req['custom_location_name']}\"")

    custom_query = query_args["custom_query"].split("|")[1:]

    assert len(custom_query) == len(expected_query)
    for i in expected_query:
        assert i in custom_query
