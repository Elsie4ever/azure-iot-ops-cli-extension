# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import Dict, List, Optional
from .common import ResourceTypeMapping

from knack.log import get_logger

from azure.cli.core.azclierror import RequiredArgumentMissingError

from .providers.rpsaas.dataprocessor.datasets import DatasetProvider
logger = get_logger(__name__)


def create_dataset(
    cmd,
    dataset_name: str,
    instance_name: str,
    resource_group_name: str,
    description: Optional[str] = None,
    payload: Optional[str] = None,
    cluster_name: Optional[str] = None,
    cluster_resource_group: Optional[str] = None,
    cluster_subscription: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    custom_location_resource_group: Optional[str] = None,
    custom_location_subscription: Optional[str] = None,
    timestamp: Optional[str] = None,
    ttl: Optional[str] = None,
    keys: Optional[str] = None,
    location: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
):
    dataset_provider = DatasetProvider(cmd)
    return dataset_provider.create(
        dataset_name=dataset_name,
        instance_name=instance_name,
        resource_group_name=resource_group_name,
        description=description,
        payload=payload,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        timestamp=timestamp,
        ttl=ttl,
        keys=keys,
        location=location,
        tags=tags,
    )


def show_dataset(
    cmd,
    dataset_name: str,
    instance_name: str,
    resource_group_name: str,
):
    dataset_provider = DatasetProvider(cmd)

    dataset_provider.resource_type = f"{ResourceTypeMapping.instance.value}/{instance_name}/datasets"
    return dataset_provider.show(
        resource_name=dataset_name,
        resource_group_name=resource_group_name,
        check_cluster_connectivity=True,
    )


def list_datasets(
    cmd,
    instance_name: str,
    resource_group_name: str,
):
    dataset_provider = DatasetProvider(cmd)

    dataset_provider.resource_type = f"{ResourceTypeMapping.instance.value}/{instance_name}/datasets"
    return dataset_provider.list(
        resource_group_name=resource_group_name
    )


def delete_dataset(
    cmd,
    dataset_name: str,
    instance_name: str,
    resource_group_name: str,
):
    dataset_provider = DatasetProvider(cmd)

    dataset_provider.resource_type = f"{ResourceTypeMapping.instance.value}/{instance_name}/datasets"
    return dataset_provider.delete(
        resource_name=dataset_name,
        resource_group_name=resource_group_name,
        check_cluster_connectivity=True,
)


def query_datasets(
    cmd,
    dataset_name: Optional[str] = None,
    instance_name: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    description: Optional[str] = None,
    keys: Optional[str] = None,
    payload: Optional[str] = None,
    timestamp: Optional[str] = None,
    ttl: Optional[str] = None,
    location: Optional[str] = None,
    resource_group_name: Optional[str] = None,
):
    dataset_provider = DatasetProvider(cmd)

    dataset_provider.resource_type = f"{ResourceTypeMapping.instance.value}/datasets"
    return dataset_provider.query(
        dataset_name=dataset_name,
        instance_name=instance_name,
        custom_location_name=custom_location_name,
        description=description,
        keys=keys,
        payload=payload,
        timestamp=timestamp,
        ttl=ttl,
        location=location,
        resource_group_name=resource_group_name,
    )
