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


def submit_pipeline(
    cmd,
    pipeline_name: str,
    resource_group_name: str,
    instance_name: Optional[str]=None,
    disabled: Optional[bool]=False,
    desc: Optional[str]=None,
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
    return pipeline_provider.submit(
        pipeline_name=pipeline_name,
        resource_group_name=resource_group_name,
        instance_name=instance_name,
        disabled=disabled,
        desc=desc,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        location=location,
        tags=tags,
    )


def visualize_pipeline(
    cmd,
    pipeline_name: str,
    resource_group_name: str,
    instance_name: Optional[str]=None,
    cluster_name: Optional[str] = None,
):
    pipeline_provider = PipelineProvider(cmd)
    return pipeline_provider.visualize(
        pipeline_name=pipeline_name,
        resource_group_name=resource_group_name,
        instance_name=instance_name,
        cluster_name=cluster_name,
    )


def add_pipeline_source_mqtt(
    cmd,
    pipeline_name: str,
    pipeline_enabled: bool,
    resource_group_name: str,
    broker: str,
    topic: List[str],
    format: List[str],
    partition_count: int,
    partition_strategy: List[str],
    name: Optional[str] = None,
    pipeline_description: Optional[str] = None,
    clean_session: Optional[bool] = None,
    authentication: Optional[List[str]] = None,
    cluster_name: Optional[str] = None,
    cluster_resource_group: Optional[str] = None,
    cluster_subscription: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    custom_location_resource_group: Optional[str] = None,
    custom_location_subscription: Optional[str] = None,
    location: Optional[str] = None,
    visualize: Optional[bool] = False,
    tags: Optional[Dict[str, str]] = None,
):
    # get instance name from cluster
    pipeline_provider = PipelineProvider(cmd)
    return pipeline_provider.add_source_mqtt(
        broker=broker,
        pipeline_name=pipeline_name,
        pipeline_enabled=pipeline_enabled,
        pipeline_description=pipeline_description,
        resource_group_name=resource_group_name,
        topic=topic,
        format=format,
        partition_count=partition_count,
        partition_strategy=partition_strategy,
        clean_session=clean_session,
        authentication=authentication,
        name=name,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        location=location,
        visualize=visualize,
        tags=tags,
    )


def add_pipeline_destination_mqtt(
    cmd,
    pipeline_name: str,
    pipeline_enabled: bool,
    resource_group_name: str,
    broker: str,
    topic: List[str],
    format: List[str],
    name: Optional[str] = None,
    qos: Optional[int] = None,
    user_property: Optional[List[str]] = None,
    retry: Optional[List[str]] = None,
    pipeline_description: Optional[str] = None,
    visualize: Optional[bool] = False,
    authentication: Optional[List[str]] = None,
    cluster_name: Optional[str] = None,
    cluster_resource_group: Optional[str] = None,
    cluster_subscription: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    custom_location_resource_group: Optional[str] = None,
    custom_location_subscription: Optional[str] = None,
    location: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
):
    # get instance name from cluster
    pipeline_provider = PipelineProvider(cmd)
    return pipeline_provider.add_destination_mqtt(
        pipeline_name=pipeline_name,
        pipeline_enabled=pipeline_enabled,
        pipeline_description=pipeline_description,
        resource_group_name=resource_group_name,
        broker=broker,
        topic=topic,
        format=format,
        qos=qos,
        user_property=user_property,
        retry=retry,
        visualize=visualize,
        authentication=authentication,
        name=name,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        location=location,
        tags=tags,
    )


def add_pipeline_processor_enrich(
    cmd,
    name: str,
    pipeline_name: str,
    next_id: str,
    resource_group_name: str,
    dataset_name: str,
    output_path: str,
    condition: Optional[List[str]] = None,
    always_array: Optional[bool] = None,
    limit: Optional[int] = None,
    pipeline_description: Optional[str] = None,
    visualize: Optional[bool] = False,
    cluster_name: Optional[str] = None,
    cluster_resource_group: Optional[str] = None,
    cluster_subscription: Optional[str] = None,
    custom_location_name: Optional[str] = None,
    custom_location_resource_group: Optional[str] = None,
    custom_location_subscription: Optional[str] = None,
    location: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
):
    # get instance name from cluster
    pipeline_provider = PipelineProvider(cmd)
    return pipeline_provider.add_processor_enrich(
        pipeline_name=pipeline_name,
        pipeline_description=pipeline_description,
        resource_group_name=resource_group_name,
        dataset_name=dataset_name,
        output_path=output_path,
        condition=condition,
        always_array=always_array,
        next_id=next_id,
        limit=limit,
        visualize=visualize,
        name=name,
        cluster_name=cluster_name,
        cluster_resource_group=cluster_resource_group,
        cluster_subscription=cluster_subscription,
        custom_location_name=custom_location_name,
        custom_location_resource_group=custom_location_resource_group,
        custom_location_subscription=custom_location_subscription,
        location=location,
        tags=tags,
    )
