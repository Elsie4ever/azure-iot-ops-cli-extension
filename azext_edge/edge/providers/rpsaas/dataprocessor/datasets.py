# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import json
import re
from typing import Dict, List, Optional

from knack.log import get_logger

from azure.cli.core.azclierror import (
    RequiredArgumentMissingError,
)
from .base import DataProcessorBaseProvider
from ....util import assemble_nargs_to_dict, build_query
from ....common import ResourceTypeMapping, AEPAuthModes

logger = get_logger(__name__)


class DatasetProvider(DataProcessorBaseProvider):
    def __init__(self, cmd):
        super(DatasetProvider, self).__init__(
            cmd=cmd,
            resource_type=ResourceTypeMapping.dataset.value,
        )

    def create(
        self,
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
        ttl: Optional[str] = "12h",
        keys: Optional[str] = None,
        location: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=custom_location_name,
            custom_location_resource_group=custom_location_resource_group,
            custom_location_subscription=custom_location_subscription,
            cluster_name=cluster_name,
            cluster_resource_group=cluster_resource_group,
            cluster_subscription=cluster_subscription
        )

        # Location
        if not location:
            location = self.get_location(resource_group_name)
        
        properties = {}

        _update_properties(
            properties,
            description=description,
            payload=payload,
            timestamp=timestamp,
            ttl=ttl,
            keys=keys,
        )

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{ResourceTypeMapping.instance.value}/{instance_name}/datasets/{dataset_name}"
        dataset_body = {
            "extendedLocation": extended_location,
            "properties": properties,
            "location": location,
            "tags": tags,
        }
        poller = self.resource_client.resources.begin_create_or_update_by_id(
            resource_id=resource_path,
            api_version=self.api_version,
            parameters=dataset_body,
        )
        return poller
    

    def query(
        self,
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
    ) -> dict:
        query = ""
        if dataset_name:
            query += f"| where name =~ \"{dataset_name}\""
        if instance_name:
            query += f"| where id contains \'{instance_name}\'"
        if custom_location_name:
            query += f"| where extendedLocation.name contains \"{custom_location_name}\""
        if description:
            query += f"| where properties.description =~ \"{description}\""
        if payload:
            query += f"| where properties.payload =~ \"{payload}\""
        if timestamp:
            query += f"| where properties.timestamp =~ \"{timestamp}\""
        if ttl:
            query += f"| where properties.ttl =~ \"{ttl}\""
        if keys:
            query += _build_keys_query(keys)

        return build_query(
            self.cmd,
            subscription_id=self.subscription,
            custom_query=query,
            location=location,
            resource_group=resource_group_name,
            type=self.resource_type,
            additional_project="extendedLocation"
        )


# Helpers
def _update_properties(
    properties,
    description: Optional[str] = None,
    payload: Optional[str] = None,
    timestamp: Optional[str] = None,
    ttl: Optional[str] = None,
    keys: Optional[str] = None,
):
    if description:
        properties["description"] = description
    if payload:
        properties["payload"] = payload
    if timestamp:
        properties["timestamp"] = timestamp
    if ttl:
        properties["ttl"] = ttl
    if keys:
        properties["keys"] = _process_dataset_keys(keys)


def _process_dataset_keys(keys: str) -> str:
    # json to list
    keys_list = json.loads(keys)
    # check if path is present in each value in keys_list
    for key in keys_list:
        if "path" not in keys_list[key]:
            raise RequiredArgumentMissingError("path is required for each key in keys.")
        
    # if no error, return json string
    return keys_list


def _build_keys_query(keys: str) -> str:
    keys_list = json.loads(keys)
    query = ""
    # for each key in keys_list, compare the keyname.path and keyname.primaryKey
    for key in keys_list:
        query += f"| where properties.keys.{key}.path =~ \"{keys_list[key]['path']}\""
        query += f"| where properties.keys.{key}.primaryKey =~ \"{keys_list[key]['primaryKey']}\""

    return query
