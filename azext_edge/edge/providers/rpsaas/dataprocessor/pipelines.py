# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from enum import Enum
import json
from uuid import uuid4
from typing import Dict, List, Optional
from .common import PIPELINE_INPUT_STAGE_PROPERTIES, PIPELINE_OUTPUT_STAGE_PROPERTIES, PIPELINE_PROCESSOR_STAGE_PROPERTIES
from azext_edge.edge.util.common import assemble_nargs_to_dict

from knack.log import get_logger
from azure.cli.core.azclierror import (
    InvalidArgumentValueError,
    RequiredArgumentMissingError,
)
from .base import DataProcessorBaseProvider
from ....util import build_query
from ....common import DPPipelineInputStageTypes, DPPipelineOutputStageTypes, DPPipelineProcessorStageTypes, ResourceTypeMapping

logger = get_logger(__name__)


class PipelineProvider(DataProcessorBaseProvider):
    def __init__(self, cmd):
        super(PipelineProvider, self).__init__(
            cmd=cmd,
            resource_type=ResourceTypeMapping.pipeline.value,
        )

    
    def create(
        self,
        enabled: bool,
        pipeline_name: str,
        instance_name: str,
        resource_group_name: str,
        input_stage: List[str],
        output_stage: str,
        aggregate_processor_stage: Optional[List[str]] = None,
        enrich_processor_stage: Optional[List[str]] = None,
        filter_processor_stage: Optional[List[str]] = None,
        grpc_processor_stage: Optional[List[str]] = None,
        http_processor_stage: Optional[List[str]] = None,
        lkv_processor_stage: Optional[List[str]] = None,
        transform_processor_stage: Optional[List[str]] = None,
        processor_stages: Optional[str] = None,
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
        
        # Properties
        # if there is no processor stages and individual processor stages, put input's next to output stage name

        properties = {
            "enabled": enabled,
            "input": _process_input_stage(input_stage),
            "stages": _process_stages(
                processor_stages=processor_stages,
                output_stage=output_stage,
                aggregate_processor_stage=aggregate_processor_stage,
                enrich_processor_stage=enrich_processor_stage,
                filter_processor_stage=filter_processor_stage,
                grpc_processor_stage=grpc_processor_stage,
                http_processor_stage=http_processor_stage,
                lkv_processor_stage=lkv_processor_stage,
                transform_processor_stage=transform_processor_stage,
            )
        }

        if description:
            properties["description"] = description

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{ResourceTypeMapping.instance.value}/{instance_name}/pipelines/{pipeline_name}"
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
    

# Helpers
def _get_required_args(
    stage_type: str,
    properties: Dict[str, any]
) -> List[str]:
    stage_properties = properties.get(stage_type)
    required_args = []
    for (prop_name, required) in stage_properties:
        if required:
            required_args.append(prop_name)
    
    return required_args


def _process_stage_args(
    stage_types: Enum,
    properties: Dict[str, any],
    stages_args: List[any]
) -> List[Dict[str, any]]:
    # if len(stages_args) > 1 and (stage_type.startswith("input") or stage_type.startswith("output")):
    #     raise InvalidArgumentValueError("Input/output stage can only have one intance.")
    
    parsed_stages = []
    # if stages_args is a list of list, process each list; if stages_args is a list, process it
    if isinstance(stages_args[0], list):
        for stage_args in stages_args:
            _evaluate_stage_args(
                parsed_stages=parsed_stages,
                stage_types=stage_types,
                stage_args=stage_args,
                properties=properties
            )
    else:
        parsed_stages = _evaluate_stage_args(
            parsed_stages=parsed_stages,
            stage_types=stage_types,
            stage_args=stages_args,
            properties=properties
        )
    
    return parsed_stages


def _evaluate_stage_args(
    parsed_stages: List[Dict[str, any]],
    stage_types: Enum,
    stage_args: List[str],
    properties: Dict[str, any]
):
    parsed_stage_args = assemble_nargs_to_dict(stage_args)
    arg_names = parsed_stage_args.keys()
    required_args = []
    type = parsed_stage_args.get("type")
    if not type:
        raise RequiredArgumentMissingError("Stage type is required but not provided.")
    stage_type = _get_stage_type(stage_types=stage_types, type=type)
    parsed_stage_args["type"] = stage_type

    stage_properties = properties.get(stage_type)
    for (prop_name, required) in stage_properties:
        # for properties with . in them, check if the first part is in arg_names, otherwise ignore even if required
        if "." in prop_name:
            if prop_name.split(".")[0] in arg_names:
                required_args.append(prop_name)
        elif required:
            required_args.append(prop_name)
    missing_args = set(required_args) - set(arg_names)
    if missing_args:
        raise RequiredArgumentMissingError(f"Stage {stage_type} is missing the {missing_args}.")
    
    # find porperties in arg_names that are not in properties
    stage_properties_names = [prop_name for (prop_name, _) in stage_properties]
    extra_args = set(arg_names) - set(stage_properties_names)
    if extra_args:
        logger.warning(f"Stage {stage_type} has extra properties {extra_args}. These will be ignored during processing.")
    
    # parsed_stage_args - extra_args
    parsed_stage_args = {k: v for k, v in parsed_stage_args.items() if k not in extra_args}
    parsed_stages.append(parsed_stage_args)

    return parsed_stages


def _get_stage_type(
    stage_types: Enum,
    type: str
) -> str:
    if type not in stage_types.__members__.keys():
        raise InvalidArgumentValueError(f"Stage type {type} is not supported.")
    else:
        return stage_types[type].value
    
    

def _process_input_stage(input_stage_properties: List[str]
    ) -> Dict[str, any]:
    # if not input_stage_properties:
    #     raise RequiredArgumentMissingError("Input stage properties are required.")
    # processed_points = []
    # required_args = {
    #     "input/http@v1": [
    #         "type",
    #         "url",
    #         "format",
    #         "interval",
    #         "partitionCount",
    #         "partitionStrategy"
    #         "next"
    #     ],
    #     "input/influxdbv2@v1": [
    #         "type",
    #         "query",
    #         "query.expression",
    #         "url",
    #         "interval",
    #         "organization",
    #         "format",
    #         "partitionCount",
    #         "partitionStrategy",
    #         "authentication",
    #         "next"
    #     ],
    #     "input/mqtt@v1": [
    #         "type",
    #         "broker",
    #         "topics",
    #         "format",
    #         "partitionCount",
    #         "partitionStrategy",
    #         "next"
    #     ],
    #     "input/sqlserver@v1": [
    #         "type",
    #         "query",
    #         "query.expression",
    #         "server",
    #         "database",
    #         "interval",
    #         "format",
    #         "partitionCount",
    #         "partitionStrategy",
    #         "next"
    #     ],
    # }

    # return processed_points
    # parsed_properties = assemble_nargs_to_dict(input_stage_properties)
    # # find required args that's not in parsed_properties
    # type = parsed_properties.get("type")
    # # if parsed_properties's type not in DPPipelineInputStageTypes enum names
    # if type not in DPPipelineInputStageTypes.__members__.keys():
    #     raise InvalidArgumentValueError(f"Input stage type {type} is not supported.")
    # else:
    #     type = DPPipelineInputStageTypes[type].value
    #     parsed_properties["type"] = type
    # required_args = _get_required_args(
    #     stage_type=type,
    #     properties=PIPELINE_INPUT_STAGE_PROPERTIES
    #     )
    # missing_args = set(required_args) - set(parsed_properties.keys())
    # if missing_args:
    #     raise RequiredArgumentMissingError(f"Input stage {type} is missing required properties {missing_args}.")
    # else:
    stages = _process_stage_args(
        stage_types=DPPipelineInputStageTypes,
        properties=PIPELINE_INPUT_STAGE_PROPERTIES,
        stages_args=input_stage_properties
    )
    return _build_stage(stages[0])


def _build_stage(
    stage: Dict[str, any]
):
    stage_type: str = stage.get("type")
    none_auth = {
        "type": "none",
    }
    if stage_type.startswith("input"):
        input_stage = {}
        for key, value in stage.items():
            if key == "next":
                input_stage["next"] = [value]
                continue
            elif key == "name":
                input_stage["displayName"] = value
                continue
            elif key == "id":
                continue
            input_stage[key] = _parse_property_value(value)
        
        if not input_stage.get("displayName"):
            input_stage["displayName"] = "input"
        
        if not input_stage.get("authentication"):
            input_stage["authentication"] = none_auth
    
        return input_stage
    elif stage_type.startswith("output"):
        output_stage = {}
        stage_id = stage.get("id") if stage.get("id") else f"{stage_type}-{str(uuid4())}"
        output_stage[stage_id] = {}
        for key, value in stage.items():
            if key == "name":
                output_stage[stage_id]["displayName"] = value
                continue
            elif key == "id":
                continue
            output_stage[stage_id][key] = _parse_property_value(value)
        
        if not output_stage[stage_id].get("displayName"):
            output_stage[stage_id]["displayName"] = "output"
        
        if not output_stage[stage_id].get("authentication"):
            output_stage[stage_id]["authentication"] = none_auth
        
        return output_stage
    elif stage_type.startswith("processor"):
        processor_stage = {}
        stage_id = stage.get("id") if stage.get("id") else f"{stage_type}-{str(uuid4())}"
        processor_stage[stage_id] = {}
        for key, value in stage.items():
            if key == "next":
                processor_stage[stage_id]["next"] = [value]
                continue
            elif key == "name":
                processor_stage[stage_id]["displayName"] = value
                continue
            elif key == "id":
                continue
            processor_stage[stage_id][key] = _parse_property_value(value)
        
        if not processor_stage[stage_id].get("displayName"):
            processor_stage[stage_id]["displayName"] = stage_id
    
        return processor_stage


def _parse_property_value(
    value: str
):
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value

def _build_input_stage(
        **kwargs
):
    input_stage = {}
    for key, value in kwargs.items():
        if key == "next":
            input_stage["next"] = [value]
            continue
        input_stage[key] = value
    
    return input_stage


def _process_stages(
    output_stage: List[str],
    processor_stages: Optional[str] = None,
    aggregate_processor_stage: Optional[List[str]] = None,
    enrich_processor_stage: Optional[List[str]] = None,
    filter_processor_stage: Optional[List[str]] = None,
    grpc_processor_stage: Optional[List[str]] = None,
    http_processor_stage: Optional[List[str]] = None,
    lkv_processor_stage: Optional[List[str]] = None,
    transform_processor_stage: Optional[List[str]] = None,
):
    stages = {}
    # cannot have both processor_stages and individual processor stages
    if processor_stages:
        stages = json.loads(processor_stages)
    elif any([
        aggregate_processor_stage,
        enrich_processor_stage,
        filter_processor_stage,
        grpc_processor_stage,
        http_processor_stage,
        lkv_processor_stage,
        transform_processor_stage
    ]):
        stages = _process_processor_stages(
            aggregate_processor_stage=aggregate_processor_stage,
            enrich_processor_stage=enrich_processor_stage,
            filter_processor_stage=filter_processor_stage,
            grpc_processor_stage=grpc_processor_stage,
            http_processor_stage=http_processor_stage,
            lkv_processor_stage=lkv_processor_stage,
            transform_processor_stage=transform_processor_stage
        )

    # output_required_args = {
    #     "output/blobstorage@v1": [
    #         "type",
    #         "accountName",
    #         "containerName",
    #         "authentication",
    #         "format"
    #     ],
    #     "output/dataexplorer@v1": [
    #         "type",
    #         "clusterUrl",
    #         "database",
    #         "table",
    #         "authentication",
    #         "columns"
    #     ],
    #     "output/fabric@v1": [
    #         "type",
    #         "workspace",
    #         "lakehouse",
    #         "table",
    #         "authentication",
    #         "columns"
    #     ],
    #     "output/file@v1": [
    #         "type",
    #         "rootDirectory",
    #         "format",
    #     ],
    #     "output/grpc@v1": [
    #         "type",
    #         "serverAddress",
    #         "rpcName",
    #         "descriptor",
    #     ],
    #     "output/http@v1": [
    #         "type",
    #         "url",
    #         "method",
    #     ],
    #     "output/mqtt@v1": [
    #         "type",
    #         "broker",
    #         "topic",
    #         "format",
    #     ],
    #     "output/refdata@v1": [
    #         "type",
    #         "dataset",
    #     ],
    # }

    # output stage
    # parsed_output_properties = assemble_nargs_to_dict(output_stage)
    # type = parsed_output_properties.get("type")
    # if type not in DPPipelineOutputStageTypes.__members__.keys():
    #     raise InvalidArgumentValueError(f"Output stage type {type} is not supported.")
    # else:
    #     type = DPPipelineOutputStageTypes[type].value
    #     parsed_output_properties["type"] = type
    # required_args = output_required_args[type]
    # missing_args = set(required_args) - set(parsed_output_properties.keys())
    # if missing_args:
    #     raise RequiredArgumentMissingError(f"Output stage {type} is missing the {missing_args}.")
    # else:
    #     stages["output"] = _build_output_stage(**parsed_output_properties)
    output_stage = _process_stage_args(
        stage_types=DPPipelineOutputStageTypes,
        properties=PIPELINE_OUTPUT_STAGE_PROPERTIES,
        stages_args=output_stage
    )

    temp: dict = _build_stage(output_stage[0])

    for key in temp:
        stages[key] = temp[key]
   
        
    return stages


def _build_output_stage(
        **kwargs
):
    output_stage = {}
    for key, value in kwargs.items():
        output_stage[key] = value
    
    return output_stage


def _process_processor_stages(
    aggregate_processor_stage: Optional[List[str]] = None,
    enrich_processor_stage: Optional[List[str]] = None,
    filter_processor_stage: Optional[List[str]] = None,
    grpc_processor_stage: Optional[List[str]] = None,
    http_processor_stage: Optional[List[str]] = None,
    lkv_processor_stage: Optional[List[str]] = None,
    transform_processor_stage: Optional[List[str]] = None,
):
    stages = {}
    # required_args = {
    #     "processor/aggregate@v1": [
    #         "windowType",
    #         "windowSize",
    #         "function",
    #         "inputPath",
    #         "outputPath",
    #         "next"
    #     ],
    #     "processor/enrich@v1": [
    #         "dataset",
    #         "outputPath",
    #         "next"
    #     ],
    #     "processor/filter@v1": [
    #         "expression",
    #         "next"
    #     ],
    #     "processor/grpc@v1": [
    #         "serverAddress",
    #         "rpcName",
    #         "descriptor",
    #         "next"
    #     ],
    #     "processor/http@v1": [
    #         "url",
    #         "method",
    #         "next"
    #     ],
    #     "processor/lkv@v1": [
    #         "inputPath",
    #         "outputPath",
    #         "next"
    #     ],
    #     "processor/transform@v1": [
    #         "expression",
    #         "next"
    #     ],
    # }
    
    # for any processor stage that exists, process it
    # flatthen the stages to a list since aggregate_processor_stage etc. can have more than one stage
    processor_stages = []

    for (s, type) in [
        (aggregate_processor_stage, "aggregate"),
        (enrich_processor_stage, "enrich"),
        (filter_processor_stage, "filter"),
        (grpc_processor_stage, "grpc"),
        (http_processor_stage, "http"),
        (lkv_processor_stage, "lkv"),
        (transform_processor_stage, "transform")
    ]:
        if s:
            for stage in s:
                # set type
                stage.append(f"type={type}")
            processor_stages.extend(s)

    stages = {}
    stage_args = _process_stage_args(
        stage_types=DPPipelineProcessorStageTypes,
        properties=PIPELINE_PROCESSOR_STAGE_PROPERTIES,
        stages_args=processor_stages
    )

    for processor_stage in stage_args:
        temp = _build_stage(processor_stage)
        for key in temp:
            stages[key] = temp[key]
    
    return stages


def _build_processor_stage(
    stage_properties: List[str]
):
    stage = {}
    for key, value in stage_properties.items():
        if key == "next":
            stage["next"] = [value]
            continue
        stage[key] = value
    
    return stage
