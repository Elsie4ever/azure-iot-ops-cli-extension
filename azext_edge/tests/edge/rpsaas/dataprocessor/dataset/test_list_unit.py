# coding=utf-8
# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Private distribution for NDA customers only. Governed by license terms at https://preview.e4k.dev/docs/use-terms/
# --------------------------------------------------------------------------------------------

from azext_edge.edge.common import ResourceTypeMapping
import pytest

from azext_edge.edge.commands_datasets import list_datasets
from azext_edge.edge.providers.rpsaas.dataprocessor.base import DATAPROCESSOR_API_VERSION

from .....generators import generate_generic_id


@pytest.mark.parametrize("mocked_send_raw_request", [
    {
        "return_value": {
            "value": [
                {"name": generate_generic_id(), "result": generate_generic_id()},
                {"name": generate_generic_id(), "result": generate_generic_id()}
            ]
        }
    }
], ids=["value"], indirect=True)
@pytest.mark.parametrize("resource_group", [None, generate_generic_id()])
def test_list_assets(
    mocked_cmd,
    mocked_send_raw_request,
    resource_group
):
    instance_name = generate_generic_id()
    result = list_datasets(
        cmd=mocked_cmd,
        instance_name=instance_name,
        resource_group_name=resource_group
    )
    assert result == mocked_send_raw_request.return_value.json.return_value["value"]
    mocked_send_raw_request.assert_called_once()
    call_kwargs = mocked_send_raw_request.call_args.kwargs
    assert call_kwargs["cli_ctx"] == mocked_cmd.cli_ctx
    assert call_kwargs["method"] == "GET"
    assert f"/providers/{ResourceTypeMapping.instance.value}/{instance_name}/datasets?api-version={DATAPROCESSOR_API_VERSION}" in call_kwargs["url"]
    assert (f"/resourceGroups/{resource_group}" in call_kwargs["url"]) is (resource_group is not None)
