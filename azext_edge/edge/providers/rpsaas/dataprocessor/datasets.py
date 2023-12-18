# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import Dict, List, Optional

from knack.log import get_logger

from azure.cli.core.azclierror import (
    InvalidArgumentValueError,
    MutuallyExclusiveArgumentError,
    RequiredArgumentMissingError,
    UnrecognizedArgumentError
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
        asset_endpoint_profile_name: str,
        resource_group_name: str,
        target_address: str,
        additional_configuration: Optional[str] = None,
        certificate_reference: Optional[List[str]] = None,
        cluster_name: Optional[str] = None,
        cluster_resource_group: Optional[str] = None,
        cluster_subscription: Optional[str] = None,
        custom_location_name: Optional[str] = None,
        custom_location_resource_group: Optional[str] = None,
        custom_location_subscription: Optional[str] = None,
        transport_authentication: Optional[str] = None,
        location: Optional[str] = None,
        password_reference: Optional[str] = None,
        username_reference: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        if certificate_reference:
            raise InvalidArgumentValueError(CERT_AUTH_NOT_SUPPORTED)
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

        auth_mode = None
        if not any([username_reference, password_reference, certificate_reference]):
            auth_mode = AEPAuthModes.anonymous.value

        # Properties - bandaid for UI so it processes no transport auth correctly
        properties = {"transportAuthentication": {"ownCertificates": []}}
        _update_properties(
            properties,
            target_address=target_address,
            additional_configuration=additional_configuration,
            auth_mode=auth_mode,
            username_reference=username_reference,
            password_reference=password_reference,
            certificate_reference=certificate_reference,
            transport_authentication=transport_authentication
        )

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{self.resource_type}/{asset_endpoint_profile_name}"
        aep_body = {
            "extendedLocation": extended_location,
            "location": location,
            "properties": properties,
            "tags": tags,
        }
        poller = self.resource_client.resources.begin_create_or_update_by_id(
            resource_id=resource_path,
            api_version=self.api_version,
            parameters=aep_body
        )
        return poller