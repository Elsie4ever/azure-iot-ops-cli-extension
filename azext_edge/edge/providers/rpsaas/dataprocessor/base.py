# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from knack.log import get_logger

from ..base_provider import RPSaaSBaseProvider
from ....common import ClusterExtensionsMapping

logger = get_logger(__name__)
DATAPROCESSOR_API_VERSION = "2023-10-04-preview"


class DataProcessorBaseProvider(RPSaaSBaseProvider):
    def __init__(
        self, cmd, resource_type: str
    ):
        super(DataProcessorBaseProvider, self).__init__(
            cmd=cmd,
            api_version=DATAPROCESSOR_API_VERSION,
            resource_type=resource_type,
            required_extension=ClusterExtensionsMapping.dataprocessor.value
        )
