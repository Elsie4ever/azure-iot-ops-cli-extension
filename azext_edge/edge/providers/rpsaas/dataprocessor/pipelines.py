# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from enum import Enum
import json
from uuid import uuid4
from typing import Dict, List, Optional, Union
from msrest.serialization import Model

from azext_edge.edge.providers.edge_api.dataprocessor import DATA_PROCESSOR_API_V1, DataProcessorResourceKinds
from .common import PIPELINE_INPUT_STAGE_PROPERTIES, PIPELINE_OUTPUT_STAGE_PROPERTIES, PIPELINE_PROCESSOR_STAGE_PROPERTIES
from azext_edge.edge.util.common import assemble_nargs_to_dict

from knack.log import get_logger
from azure.cli.core.azclierror import (
    InvalidArgumentValueError,
    RequiredArgumentMissingError,
)
from .base import DataProcessorBaseProvider, MicroObjectCache
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
    

    def submit(
        self,
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
        # Location
        if not location:
            location = self.get_location(resource_group_name)
        
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=custom_location_name,
            custom_location_resource_group=custom_location_resource_group,
            custom_location_subscription=custom_location_subscription,
            cluster_name=cluster_name,
            cluster_resource_group=cluster_resource_group,
            cluster_subscription=cluster_subscription
        )

        if not instance_name:
            instance_name = self.get_dp_instance_name(extended_location=extended_location)

        cache = MicroObjectCache(self.cmd, object)
        cache_resource_name = _get_cache_entry_name(pipeline_name=pipeline_name, instance_name=instance_name)
        cache_serialization_model = "object"
        cached_import: Union[object, None] = cache.get(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            serialization_model=cache_serialization_model,
        )
        update_to_import = cached_import if cached_import else {}

        if not update_to_import:
            raise RequiredArgumentMissingError("No pipeline data found in cache. Please create a pipeline first.")
        
        ordered, obsolete = _print_as_tree(update_to_import)
        update_to_import = _reorder_stages(update_to_import, ordered, obsolete)

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{ResourceTypeMapping.instance.value}/{instance_name}/pipelines/{pipeline_name}"
        dataset_body = {
            "extendedLocation": extended_location,
            "properties": {
                "enabled": not disabled,
                "description": desc,
                "input": update_to_import["properties"]["input"],
                "stages": update_to_import["properties"]["stages"]
            },
            "location": location,
            "tags": tags,
        }
        poller = self.resource_client.resources.begin_create_or_update_by_id(
            resource_id=resource_path,
            api_version=self.api_version,
            parameters=dataset_body,
        )
        return poller
    

    def visualize(
        self,
        pipeline_name: str,
        resource_group_name: str,
        instance_name: Optional[str]=None,
        cluster_name: Optional[str] = None,
    )-> Dict[str, any]:
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=None,
            custom_location_resource_group=None,
            custom_location_subscription=None,
            cluster_name=cluster_name,
            cluster_resource_group=None,
            cluster_subscription=None
        )

        if not instance_name:
            instance_name = self.get_dp_instance_name(extended_location=extended_location)

        cache = MicroObjectCache(self.cmd, object)
        cache_resource_name = _get_cache_entry_name(pipeline_name=pipeline_name, instance_name=instance_name)
        cache_serialization_model = "object"
        cached_import: Union[object, None] = cache.get(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            serialization_model=cache_serialization_model,
        )
        update_to_import = cached_import if cached_import else {}

        if not update_to_import:
            raise RequiredArgumentMissingError("No pipeline data found in cache. Please create a pipeline first.")

        _print_as_tree(update_to_import, True)
    

    def add_source_mqtt(
        self,
        pipeline_name: str,
        broker: str,
        resource_group_name: str,
        topic: List[str],
        format: List[str],
        partition_count: int,
        partition_strategy: List[str],
        name: Optional[str] = None,
        pipeline_enabled: Optional[bool] = True,
        pipeline_description: Optional[str] = None,
        visualize: Optional[bool] = False,
        clean_session: Optional[bool] = None,
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
        

        def _get_cache_entry_name(pipeline_name: str, instance_name: str):
            return f"{instance_name}_{pipeline_name}"

        # Location
        if not location:
            location = self.get_location(resource_group_name)
        
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=custom_location_name,
            custom_location_resource_group=custom_location_resource_group,
            custom_location_subscription=custom_location_subscription,
            cluster_name=cluster_name,
            cluster_resource_group=cluster_resource_group,
            cluster_subscription=cluster_subscription
        )

        # get instance name from cluster
        # instance_name = DATA_PROCESSOR_API_V1.get_resources(DataProcessorResourceKinds.INSTANCE)
        instance_name = self.get_dp_instance_name(extended_location=extended_location)

        cache = MicroObjectCache(self.cmd, object)
        cache_resource_name = _get_cache_entry_name(pipeline_name=pipeline_name, instance_name=instance_name)
        cache_serialization_model = "object"
        cached_import: Union[object, None] = cache.get(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            serialization_model=cache_serialization_model,
        )
        update_to_import = cached_import if cached_import else {}

        input_stage = {
            "displayName": name if name else "input",
            "type": "input/mqtt@v1",
            "broker": broker,
            "topics": topic,
            "format": _process_format(format),
            "partitionCount": int(partition_count),
            "partitionStrategy": _process_partition_stategy(partition_strategy),
            "authentication": _process_authentication(authentication),
        }

        if not update_to_import:
            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "input": input_stage
                },
                "location": location,
                "tags": tags,
            }
        else:
            update_to_import["properties"]["input"] = input_stage
            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "input": update_to_import["properties"]["input"],
                    "stages": update_to_import["properties"].get("stages", {})
                },
                "location": location,
                "tags": tags,
            }

        if pipeline_description:
            update_to_import["description"] = pipeline_description
        
        if clean_session:
            update_to_import["input"]["cleanSession"] = clean_session

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{ResourceTypeMapping.instance.value}/{instance_name}/pipelines/{pipeline_name}"
        body = {
            "extendedLocation": extended_location,
            "properties": update_to_import["properties"],
            "location": location,
            "tags": tags,
        }
        update_to_import.update(body)

        ordered, obsoletes = _print_as_tree(update_to_import, visualize)

        # store to az cache
        cached = cache.set(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            payload=update_to_import,
            serialization_model=cache_serialization_model,
        )
        if not visualize:
            return cached
        # else:
        #     if pipeline_enabled is None:
        #         raise RequiredArgumentMissingError("Pipeline enabled is required.")

        #     update_to_import = _reorder_stages(update_to_import, ordered, obsoletes)

        #     poller = self.resource_client.resources.begin_create_or_update_by_id(
        #         resource_id=resource_path,
        #         api_version=self.api_version,
        #         parameters=update_to_import,
        #     )
        #     return poller


    def add_destination_mqtt(
        self,
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

        # Location
        if not location:
            location = self.get_location(resource_group_name)
        
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=custom_location_name,
            custom_location_resource_group=custom_location_resource_group,
            custom_location_subscription=custom_location_subscription,
            cluster_name=cluster_name,
            cluster_resource_group=cluster_resource_group,
            cluster_subscription=cluster_subscription
        )

        instance_name = self.get_dp_instance_name(extended_location=extended_location)

        cache = MicroObjectCache(self.cmd, object)
        cache_resource_name = _get_cache_entry_name(pipeline_name=pipeline_name, instance_name=instance_name)
        cache_serialization_model = "object"
        cached_import: Union[object, None] = cache.get(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            serialization_model=cache_serialization_model,
        )
        update_to_import = cached_import if cached_import else {}

        output_stage = {
            "displayName": name if name else "output",
            "type": "output/mqtt@v1",
            "broker": broker,
            "topic": _process_topic(topic),
            "format": _process_format(format),
            "authentication": _process_authentication(authentication),
        }

        if not update_to_import:
            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "stages": {
                        "output": output_stage
                    }
                },
                "location": location,
                "tags": tags,
            }
        else:
            stages = update_to_import["properties"].get("stages", {})
            stages["output"] = output_stage

            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "input": update_to_import["properties"]["input"],
                    "stages": stages
                },
                "location": location,
                "tags": tags,
            }

        if pipeline_description:
            update_to_import["properties"]["description"] = pipeline_description
        
        if qos:
            update_to_import["properties"]["output"]["qos"] = qos

        if user_property:
            update_to_import["properties"]["output"]["userProperties"] = _process_user_property(user_property)
        
        if retry:
            update_to_import["properties"]["output"]["retry"] = _process_retry(retry)

        resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
            f"{ResourceTypeMapping.instance.value}/{instance_name}/pipelines/{pipeline_name}"

        ordered, obsoletes = _print_as_tree(update_to_import, visualize)

        # store to az cache
        cached = cache.set(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            payload=update_to_import,
            serialization_model=cache_serialization_model,
        )
        if not visualize:
            return cached
            
        # else:
        #     update_to_import = _reorder_stages(update_to_import, ordered, obsoletes)

        #     # ask user to confirm the update, if yes, update the pipeline
        #     if not as_tree:
        #         print(json.dumps(update_to_import, indent=4))

        #     # ask user
        #     print("If you are satisfied with the changes, type 'y' to confirm the pipeline creation, otherwise type 'n' to cancel. (y/n)")
        #     answer = input()

        #     if answer.lower() == "y":
        #         poller = self.resource_client.resources.begin_create_or_update_by_id(
        #             resource_id=resource_path,
        #             api_version=self.api_version,
        #             parameters=update_to_import,
        #         )
        
        #         return poller
    

    def add_processor_enrich(
        self,
        name: str,
        pipeline_name: str,
        resource_group_name: str,
        dataset_name: str,
        output_path: str,
        next_id: str,
        pipeline_enabled: Optional[bool] = True,
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
        # Location
        if not location:
            location = self.get_location(resource_group_name)
        
        extended_location = self.check_cluster_and_custom_location(
            custom_location_name=custom_location_name,
            custom_location_resource_group=custom_location_resource_group,
            custom_location_subscription=custom_location_subscription,
            cluster_name=cluster_name,
            cluster_resource_group=cluster_resource_group,
            cluster_subscription=cluster_subscription
        )

        # get instance name from cluster
        # instance_name = DATA_PROCESSOR_API_V1.get_resources(DataProcessorResourceKinds.INSTANCE)
        instance_name = self.get_dp_instance_name(extended_location=extended_location)

        cache = MicroObjectCache(self.cmd, object)
        cache_resource_name = _get_cache_entry_name(pipeline_name=pipeline_name, instance_name=instance_name)
        cache_serialization_model = "object"
        cached_import: Union[object, None] = cache.get(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            serialization_model=cache_serialization_model,
        )

        update_to_import = cached_import if cached_import else {}

        id = _generate_stage_name("enrich")

        stage = {
            "displayName": name if name else id,
            "type": "processor/enrich@v1",
            "dataset": dataset_name,
            "outputPath": output_path,
            "next": next_id
        }

        if not update_to_import:
            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "stages": {
                        f"{id}": stage
                    }
                },
                "location": location,
                "tags": tags,
            }
        else:
            # find id using name
            if name:
                old_id = [key for key, value in update_to_import["properties"]["stages"].items() if value["displayName"] == name]
                id = old_id[0] if old_id else id
            update_to_import["properties"]["stages"][id] = stage
            update_to_import = {
                "extendedLocation": extended_location,
                "properties": {
                    "enabled": pipeline_enabled,
                    "stages": update_to_import["properties"]["stages"],
                    "input": update_to_import["properties"].get("input", {}),
                },
                "location": location,
                "tags": tags,
            }

        if condition:
            update_to_import["properties"]["conditions"] = _process_condition(condition)

        if always_array:
            update_to_import["properties"]["alwaysArray"] = always_array

        if limit:
            update_to_import["properties"]["limit"] = limit

        if pipeline_description:
            update_to_import["properties"]["description"] = pipeline_description

        ordered, obsoletes=_print_as_tree(update_to_import, visualize)

        # store to az cache
        cached = cache.set(
            resource_name=cache_resource_name,
            resource_group=resource_group_name,
            resource_type=ResourceTypeMapping.pipeline.value,
            payload=update_to_import,
            serialization_model=cache_serialization_model,
        )
        if not visualize:
            return cached
            
        # else:
        #     update_to_import = _reorder_stages(update_to_import, ordered, obsoletes)
        #     resource_path = f"/subscriptions/{self.subscription}/resourceGroups/{resource_group_name}/providers/"\
        #     f"{ResourceTypeMapping.instance.value}/{instance_name}/pipelines/{pipeline_name}"
        #     # ask user to confirm the update, if yes, update the pipeline
        #     if not as_tree:
        #         print(json.dumps(update_to_import, indent=4))

        #     # ask user
        #     print("\nIf you are satisfied with the changes, type 'y' to confirm the pipeline creation, otherwise type 'n' to cancel. (y/n)")
        #     answer = input()

        #     if answer.lower() == "y":
        #         poller = self.resource_client.resources.begin_create_or_update_by_id(
        #             resource_id=resource_path,
        #             api_version=self.api_version,
        #             parameters=update_to_import,
        #         )
        
        #         return poller


def _process_format(
    format: List[str]
):
    parsed_format = assemble_nargs_to_dict(format)
    # check type
    type = parsed_format.get("type", None)
    if not type:
        raise RequiredArgumentMissingError("Format type is required.")
    
    return parsed_format


def _process_topic(
    topic: List[str]
):
    parsed_topic = assemble_nargs_to_dict(topic)
    # check type
    type = parsed_topic.get("type", None)
    if not type:
        raise RequiredArgumentMissingError("Topic type is required.")
    
    value = parsed_topic.get("value", None)
    if not value:
        raise RequiredArgumentMissingError("Topic value is required.")
    
    return parsed_topic


def _process_retry(
    retry: List[str]
):
    parsed_retry = assemble_nargs_to_dict(retry)
    # check type
    type = parsed_retry.get("type", None)
    if not type:
        raise RequiredArgumentMissingError("Retry type is required.")
    
    return parsed_retry


def _process_partition_stategy(
    partition_strategy: List[str]
):
    parsed_partition_strategy = assemble_nargs_to_dict(partition_strategy)
    # check type
    type = parsed_partition_strategy.get("type", None)
    if not type:
        raise RequiredArgumentMissingError("Partition strategy type is required.")
    
    if type in ["id", "key"]:
        expression = parsed_partition_strategy.get("expression", None)
        if not expression:
            raise RequiredArgumentMissingError("Partition strategy expression is required.")
    
    return parsed_partition_strategy


def _process_authentication(
    authentication: Optional[List[str]] = None
):
    if not authentication:
        return {"type": "none"}
    parsed_authentication = assemble_nargs_to_dict(authentication)
    # check type
    type = parsed_authentication.get("type", None)
    if not type or type not in ["none", "usernamePassword", "serviceAccountToken"]:
        raise InvalidArgumentValueError("Authentication type is required and must be one of 'none', 'usernamePassword', 'serviceAccountToken'.")
    
    if type == "usernamePassword":
        username = parsed_authentication.get("username", None)
        password = parsed_authentication.get("password", None)
        if not username or not password:
            raise RequiredArgumentMissingError("Username and password are required for usernamePassword authentication.")
    
    return parsed_authentication


def _process_condition(
    condition: List[str]
):
    parsed_conditions = assemble_nargs_to_dict(condition)
    # check type
    for condition in parsed_conditions:
        type = condition.get("type", None)
        if not type:
            raise RequiredArgumentMissingError("Condition type is required.")
        
        input_path = condition.get("inputPath", None)
        if not input_path:
            raise RequiredArgumentMissingError("Condition inputPath is required.")
        
    return parsed_conditions


def _process_user_property(
    user_property: List[str]
):
    parsed_user_properties = assemble_nargs_to_dict(user_property)
    # check type
    for user_property in parsed_user_properties:
        key = user_property.get("key", None)
        if not key:
            raise RequiredArgumentMissingError("User property key is required.")
        
        value = user_property.get("value", None)
        if not value:
            raise RequiredArgumentMissingError("User property value is required.")
    
    return parsed_user_properties
    
    

def _generate_stage_name(
    stage_type: str
) -> str:
    # generate 6-charactors hexdecimal string
    return f"{stage_type}_{uuid4().hex[:6]}"


def _reorder_stages(
    update_to_import: Dict[str, any],
    ordered: List[str],
    obsoletes: List[str]
):
    # reorder stages in update_to_import
    properties = update_to_import.get("properties")
    input = properties.get("input", {})
    stages: dict = properties.get("stages", {})

    # remove obsoletes from stages
    for obesolete in obsoletes:
        stages.pop(obesolete, None)

    # reassign "next" property of stages
    if input:
        input["next"] = [ordered[1]]
        ordered.pop(0)
    for index, stage in enumerate(ordered):
        if index < len(ordered) - 1:
            stages[stage]["next"] = [ordered[index + 1]]
    
    return update_to_import

        


def _get_cache_entry_name(pipeline_name: str, instance_name: str):
    return f"{instance_name}_{pipeline_name}"
        

def _print_as_tree(
    update_to_import: Dict[str, any],
    as_tree: Optional[bool] = False
):
    # build tree from update_to_import with
    # --------input stage(mqtt): {stage_name}
    # |
    # |
    # --------processor stage(enrichment): {stage_name}
    # |
    # |
    # --------output stage(mqtt): {stage_name}
    properties = update_to_import.get("properties")
    input = properties.get("input", {})
    stages: dict = properties.get("stages", {})
    if as_tree:
        if input:
            print(f"+-------  input stage({input.get('type')}): {input.get('displayName')}")
        else:
            # print in red text: missing input stage
            print("+-------  input stage: \033[91mMISSING\033[0m")

    # get output stage that match type starts with "output"
    output_stage = []
    for stage in stages:
        if stages[stage].get("type").startswith("output"):
            output_stage.append(stages[stage])
    sorted, obesoletes = _sort_by_next(stages)
    if as_tree:
        for stage in sorted:
            if stages[stage].get("type").startswith("processor"):
                print("|")
                print("|")
                print(f"+-------  processor stage({stages[stage].get('type')}): {stages[stage].get('displayName')}[{stage}]")
        print("|")
        print("|")

    if len(output_stage) > 0 and "output" in obesoletes:
        # remove from obesoletes
        obesoletes.remove("output")

    if as_tree:
        if output_stage and output_stage[0].get("type").startswith("output"):
            print(f"+-------  output stage({output_stage[0].get('type')}): {output_stage[0].get('displayName')}[output]")
        else:
            # print in red text: missing output stage
            print("+-------  output stage: \033[91mMISSING\033[0m")
        
        if obesoletes:
            print("\n\n\033[93mOrphaned stages:\033[0m")
            for obesolete in obesoletes:
                print(f"{stages[obesolete].get('type')} stage: {stages[obesolete].get('displayName')}[{obesolete}]")
    
    # update sorted list to include input stage and output stage if there is any
    if input:
        sorted.insert(0, "input")
    if output_stage:
        sorted.append("output")

    return sorted, obesoletes
    


def _sort_by_next(
    stages: Dict[str, any]
):
    # sort the stages by "next" pointer
    # find obsolete stages that next stage does not exist

    obesoletes = []
    sorted:list = list(stages.copy())
    for index, stage in enumerate(stages):
        next_id = stages[stage].get("next")
        # find id from displayName
        # next_id = [key for key, value in stages.items() if value.get("displayName") == next_name]
        # next_id = next_id[0] if next_id else ""
        # if key of stage equals next_id, add to sorted
        if next_id in stages:
            if not stages[next_id].get("type").startswith("output"):
                sorted.pop(index)
                sorted = sorted[:sorted.index(next_id)] + [stage] + sorted[sorted.index(next_id):]
        else:
            obesoletes.append(stage)

    for obesolete in obesoletes.copy():
        output_stage = [stage for stage in stages if stages[stage].get("type").startswith("output")]
        if output_stage and stages[obesolete].get("next", "").startswith("output"):
            # remove from obesoletes
            obesoletes.remove(obesolete)
    sorted = [stage for stage in sorted if stage not in obesoletes]
    
    return sorted, obesoletes


        


            


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
