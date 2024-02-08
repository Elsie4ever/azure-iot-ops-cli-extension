# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import Dict, List, Optional

from knack.log import get_logger
from .base import DataProcessorBaseProvider
from ....util import build_query
from ....common import ResourceTypeMapping, AEPAuthModes

logger = get_logger(__name__)


class InstanceProvider(DataProcessorBaseProvider):
    def __init__(self, cmd):
        super(InstanceProvider, self).__init__(
            cmd=cmd,
            resource_type=ResourceTypeMapping.instance.value,
        )


    def query (
        self,
        instance_name: Optional[str] = None,
        custom_location_name: Optional[str] = None,
        description: Optional[str] = None,
        location: Optional[str] = None,
        resource_group_name: Optional[str] = None,
    ) -> dict:
        
        query = ""
        if instance_name:
            query += f"| where name contains \'{instance_name}\'"
        if custom_location_name:
            query += f"| where extendedLocation.name contains \"{custom_location_name}\""
        if description:
            query += f"| where properties.description =~ \"{description}\""

        return build_query(
            self.cmd,
            subscription_id=self.subscription,
            custom_query=query,
            location=location,
            resource_group=resource_group_name,
            type=self.resource_type,
            additional_project="extendedLocation"
        )