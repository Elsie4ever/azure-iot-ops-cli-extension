# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import pytest

from azext_edge.edge.commands_datasets import delete_dataset
from azext_edge.edge.common import ResourceTypeMapping
from azext_edge.edge.providers.rpsaas.dataprocessor.base import DATAPROCESSOR_API_VERSION

from .....generators import generate_generic_id


@pytest.mark.parametrize("mocked_resource_management_client", [
    {"resources.get": {"extendedLocation": {"name": generate_generic_id()}}}
], ids=["resources.get"], indirect=True)
@pytest.mark.parametrize("resource_group", [None, generate_generic_id()])
def test_delete_dataset(
    mocked_cmd,
    mock_check_cluster_connectivity,
    mocked_resource_management_client,
    resource_group
):
    dataset_name = generate_generic_id()
    instance_name = generate_generic_id()
    resource_group = generate_generic_id()
    result = delete_dataset(
        cmd=mocked_cmd,
        dataset_name=dataset_name,
        instance_name=instance_name,
        resource_group_name=resource_group
    )

    assert result is None
    mocked_resource_management_client.resources.begin_delete.assert_called_once()
    call_kwargs = mocked_resource_management_client.resources.begin_delete.call_args.kwargs
    assert call_kwargs["resource_group_name"] == resource_group
    assert call_kwargs["resource_type"] == f"{ResourceTypeMapping.instance.value}/{instance_name}/datasets"
    assert call_kwargs["parent_resource_path"] == ""
    assert call_kwargs["resource_provider_namespace"] == ""
    assert call_kwargs["resource_name"] == dataset_name
    assert call_kwargs["api_version"] == DATAPROCESSOR_API_VERSION
