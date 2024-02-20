# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import Dict, List, Optional
from .common import ResourceTypeMapping

from knack.log import get_logger

from azure.cli.core.azclierror import RequiredArgumentMissingError

from .providers.rpsaas.dataprocessor.pipelines import PipelineProvider
logger = get_logger(__name__)


def create_pipeline(
    cmd,
    enabled: bool,
    pipeline_name: str,
    instance_name: str,
    # expand input stage properties?
    input_stage: List[str],
    output_stage: str,
    resource_group_name: str,
    processor_stages: Optional[str] = None,
    aggregate_processor_stage: Optional[List[str]] = None,
    enrich_processor_stage: Optional[List[str]] = None,
    filter_processor_stage: Optional[List[str]] = None,
    grpc_processor_stage: Optional[List[str]] = None,
    http_processor_stage: Optional[List[str]] = None,
    lkv_processor_stage: Optional[List[str]] = None,
    transform_processor_stage: Optional[List[str]] = None,
    description: Optional[str] = None,
    cluster_name: Optional[str] = None,
    cluster_resource_group: Optional[str] = None,
    cluster_subscription: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    custom_location_resource_group: Optional[str] = None,
    custom_location_subscription: Optional[str] = None,
    location: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
):
    pipeline_provider = PipelineProvider(cmd)
    return pipeline_provider.create(
        enabled=enabled,
        pipeline_name=pipeline_name,
        instance_name=instance_name,
        input_stage=input_stage,
        output_stage=output_stage,
        resource_group_name=resource_group_name,
        processor_stages=processor_stages,
        aggregate_processor_stage=aggregate_processor_stage,
        enrich_processor_stage=enrich_processor_stage,
        filter_processor_stage=filter_processor_stage,
        grpc_processor_stage=grpc_processor_stage,
        http_processor_stage=http_processor_stage,
        lkv_processor_stage=lkv_processor_stage,
        transform_processor_stage=transform_processor_stage,
        description=description,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        location=location,
        tags=tags,
    )
