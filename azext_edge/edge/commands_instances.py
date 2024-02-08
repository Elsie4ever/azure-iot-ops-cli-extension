# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import Dict, List, Optional

from knack.log import get_logger

from azure.cli.core.azclierror import RequiredArgumentMissingError

from .providers.rpsaas.dataprocessor.instances import InstanceProvider
logger = get_logger(__name__)


def list_instances(
    cmd,
    resource_group_name: Optional[str] = None,
) -> dict:
    instance_provider = InstanceProvider(cmd)
    return instance_provider.list(resource_group_name=resource_group_name)


def show_instance(
    cmd,
    instance_name: str,
    resource_group_name: str,
) -> dict:
    instance_provider = InstanceProvider(cmd)
    return instance_provider.show(
        resource_name=instance_name,
        resource_group_name=resource_group_name,
        check_cluster_connectivity=True
    )

def query_instances(
    cmd,
    instance_name: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    description: Optional[str] = None,
    location: Optional[str] = None,
    resource_group_name: Optional[str] = None,
) -> dict:
    instance_provider = InstanceProvider(cmd)
    return instance_provider.query(
        instance_name=instance_name,
        custom_location_name=custom_location_name,
        description=description,
        location=location,
        resource_group_name=resource_group_name
    )
