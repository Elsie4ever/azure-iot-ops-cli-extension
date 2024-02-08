# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import pytest

from azure.cli.core.azclierror import (
    RequiredArgumentMissingError
)

from azext_edge.edge.commands_datasets import create_dataset
from azext_edge.edge.common import ResourceTypeMapping
from azext_edge.edge.providers.rpsaas.dataprocessor.base import DATAPROCESSOR_API_VERSION

from .....generators import generate_generic_id
from .conftest import generate_keys


@pytest.mark.parametrize("mocked_resource_management_client", [{
    "resource_groups.get": {"location": generate_generic_id()},
    "resources.begin_create_or_update_by_id": {"result": generate_generic_id()}
}], ids=["create"], indirect=True)
@pytest.mark.parametrize("dataset_helpers_fixture", [{
    "update_properties": generate_generic_id(),
}], ids=["create helpers"], indirect=True)
@pytest.mark.parametrize("req", [
    {},
    {
        "keys": generate_keys([("path1", True), ("path2", False)]),
    },
    {
        "description": generate_generic_id(),
        "payload": generate_generic_id(),
        "cluster_name": generate_generic_id(),
        "cluster_resource_group": generate_generic_id(),
        "cluster_subscription": generate_generic_id(),
        "custom_location_name": generate_generic_id(),
        "custom_location_resource_group": generate_generic_id(),
        "custom_location_subscription": generate_generic_id(),
        "timestamp": generate_generic_id(),
        "ttl": generate_generic_id(),
        "keys": generate_keys([("path1", False), ("path2", False)]),
        "tags": generate_generic_id(),
    },
])
def test_create_asset(mocker, mocked_cmd, mocked_resource_management_client, dataset_helpers_fixture, req):
    [patched_up] = dataset_helpers_fixture
    patched_cap = mocker.patch(
        "azext_edge.edge.providers.rpsaas.dataprocessor.base.DataProcessorBaseProvider.check_cluster_and_custom_location"
    )
    patched_cap.return_value = generate_generic_id()

    # Required params
    dataset_name = generate_generic_id()
    instance_name = generate_generic_id()
    resource_group_name = generate_generic_id()

    result = create_dataset(
        cmd=mocked_cmd,
        dataset_name=dataset_name,
        instance_name=instance_name,
        resource_group_name=resource_group_name,
        **req
    ).result()

    # resource group call
    location = req.get(
        "location",
        mocked_resource_management_client.resource_groups.get.return_value.as_dict.return_value["location"]
    )
    if req.get("location"):
        mocked_resource_management_client.resource_groups.get.assert_not_called()
    else:
        mocked_resource_management_client.resource_groups.get.assert_called_once()

    # create call
    poller = mocked_resource_management_client.resources.begin_create_or_update_by_id.return_value
    assert result == poller.result()
    mocked_resource_management_client.resources.begin_create_or_update_by_id.assert_called_once()
    call_kwargs = mocked_resource_management_client.resources.begin_create_or_update_by_id.call_args.kwargs
    expected_resource_path = f"/resourceGroups/{resource_group_name}/providers/{ResourceTypeMapping.instance.value}"\
        f"/{instance_name}/datasets/{dataset_name}"
    assert expected_resource_path in call_kwargs["resource_id"]
    assert call_kwargs["api_version"] == DATAPROCESSOR_API_VERSION

    # asset body
    request_body = call_kwargs["parameters"]
    assert request_body["location"] == location
    assert request_body["tags"] == req.get("tags")
    assert request_body["extendedLocation"] == patched_cap.return_value

    # Extended location helper call
    for arg in patched_cap.call_args.kwargs:
        expected_arg = mocked_cmd.cli_ctx.data['subscription_id'] if arg == "subscription" else req.get(arg)
        assert patched_cap.call_args.kwargs[arg] == expected_arg

    # Properties
    request_props = request_body["properties"]

    # Check that update props mock got called correctly
    assert request_props["result"]
    for arg in patched_up.call_args.kwargs:
        assert patched_up.call_args.kwargs[arg] == req.get(arg)
        assert request_props.get(arg) is None


def test_create_asset_error(mocked_cmd):
    with pytest.raises(RequiredArgumentMissingError):
        create_dataset(
            cmd=mocked_cmd,
            dataset_name=generate_generic_id(),
            instance_name=generate_generic_id(),
            resource_group_name=generate_generic_id(),
        )
