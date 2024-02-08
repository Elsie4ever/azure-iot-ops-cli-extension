# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import List
import pytest
from .....generators import generate_generic_id

# Paths for mocking
DATASETS_PATH = "azext_edge.edge.providers.rpsaas.dataprocessor.datasets"

@pytest.fixture()
def dataset_helpers_fixture(mocker, request):
    # TODO: see if there is a nicer way to mass mock helper funcs
    helper_fixtures = []

    def mock_update_properties(properties, **kwargs):
        """Minimize how much to check by setting everything update properties should touch to None."""
        for k in kwargs:
            properties.pop(k, None)
        properties["result"] = request.param["update_properties"]

    patched_up = mocker.patch(f"{DATASETS_PATH}._update_properties")
    patched_up.side_effect = mock_update_properties
    helper_fixtures.append(patched_up)
    yield helper_fixtures


def generate_keys(values: List[tuple]):
    keys = {}
    for value in values:
        key_name = generate_generic_id()
        keys[key_name] = {
            "path": value[0],
            "value": value[1]
        }
    
    return keys

# Generic objects
MINIMUM_DATASET = {
    "extendedLocation": {
        "name": generate_generic_id(),
        "type": generate_generic_id(),
    },
    "id": generate_generic_id(),
    "location": "westus3",
    "name": "props-test-min",
    "properties": {
        "provisioningState": "Accepted",
    },
    "resourceGroup": generate_generic_id(),
    "type": "microsoft.iotoperationsdataprocessor/instances/datasets"
}
FULL_DATASET = {
    "extendedLocation": {
        "name": generate_generic_id(),
        "type": generate_generic_id(),
    },
    "id": generate_generic_id(),
    "location": "westus3",
    "name": "props-test-max",
    "properties": {
        "description": generate_generic_id(),
        "payload": generate_generic_id(),
        "timestamp": generate_generic_id(),
        "ttl": generate_generic_id(),
        "keys": generate_keys([("path1", False), ("path2", False)]),
        "provisioningState": "Accepted",
    },
    "resourceGroup": generate_generic_id(),
    "tags": {
        generate_generic_id(): generate_generic_id(),
        generate_generic_id(): generate_generic_id()
    },
    "type": "microsoft.deviceregistry/assets"
}
