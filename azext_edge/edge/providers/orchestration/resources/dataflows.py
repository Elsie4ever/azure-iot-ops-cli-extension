# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from typing import TYPE_CHECKING, Iterable

from knack.log import get_logger
from rich.console import Console
from azure.cli.core.azclierror import InvalidArgumentValueError

from azext_edge.edge.providers.orchestration.common import AUTHENTICATION_TYPE_REQUIRED_PARAMS, DATAFLOW_ENDPOINT_AUTHENTICATION_TYPE_MAP, DATAFLOW_ENDPOINT_TYPE_REQUIRED_PARAMS, DATAFLOW_ENDPOINT_TYPE_SETTINGS, DataflowEndpointType, DataflowEndpointAuthenticationType, DataflowOperationType
from azext_edge.edge.providers.orchestration.resources.instances import Instances
from azext_edge.edge.providers.orchestration.resources.reskit import GetInstanceExtLoc, get_file_config
from azext_edge.edge.util.common import should_continue_prompt

from ....util.az_client import get_iotops_mgmt_client, wait_for_terminal_state
from ....util.queryable import Queryable

logger = get_logger(__name__)

console = Console()


if TYPE_CHECKING:
    from ....vendor.clients.iotopsmgmt.operations import (
        DataflowEndpointOperations,
        DataflowOperations,
        DataflowProfileOperations,
    )


class DataFlowProfiles(Queryable):
    def __init__(self, cmd):
        super().__init__(cmd=cmd)
        self.iotops_mgmt_client = get_iotops_mgmt_client(
            subscription_id=self.default_subscription_id,
        )
        self.ops: "DataflowProfileOperations" = self.iotops_mgmt_client.dataflow_profile
        self.instances = Instances(cmd=cmd)
        self.dataflows = DataFlows(cmd=cmd)

    def show(self, name: str, instance_name: str, resource_group_name: str) -> dict:
        return self.ops.get(
            resource_group_name=resource_group_name, instance_name=instance_name, dataflow_profile_name=name
        )

    def list(self, instance_name: str, resource_group_name: str) -> Iterable[dict]:
        return self.ops.list_by_resource_group(resource_group_name=resource_group_name, instance_name=instance_name)


class DataFlows(Queryable):
    def __init__(self, cmd):
        super().__init__(cmd=cmd)
        self.iotops_mgmt_client = get_iotops_mgmt_client(
            subscription_id=self.default_subscription_id,
        )
        self.ops: "DataflowOperations" = self.iotops_mgmt_client.dataflow
        self.instances = Instances(self.cmd)

    def show(self, name: str, dataflow_profile_name: str, instance_name: str, resource_group_name: str) -> dict:
        return self.ops.get(
            resource_group_name=resource_group_name,
            instance_name=instance_name,
            dataflow_profile_name=dataflow_profile_name,
            dataflow_name=name,
        )

    def list(self, dataflow_profile_name: str, instance_name: str, resource_group_name: str) -> Iterable[dict]:
        return self.ops.list_by_profile_resource(
            resource_group_name=resource_group_name,
            instance_name=instance_name,
            dataflow_profile_name=dataflow_profile_name,
        )

    def apply(
        self,
        name: str,
        dataflow_profile_name: str,
        instance_name: str,
        resource_group_name: str,
        config_file: str,
        **kwargs
    ) -> dict:
        resource = {}
        dataflow_config = get_file_config(config_file)
        self.instance = self.instances.show(name=instance_name, resource_group_name=resource_group_name)
        resource["extendedLocation"] = self.instance["extendedLocation"]
        resource["properties"] = dataflow_config

        # Validation for the config file
        self._validate_dataflow_config(
            dataflow_config=dataflow_config,
            instance_name=instance_name,
            resource_group_name=resource_group_name,
        )

        with console.status("Working..."):
            poller = self.ops.begin_create_or_update(
                dataflow_profile_name=dataflow_profile_name,
                dataflow_name=name,
                instance_name=instance_name,
                resource_group_name=resource_group_name,
                resource=resource,
            )
            return wait_for_terminal_state(poller, **kwargs)
    
    def delete(
        self,
        name: str,
        dataflow_profile_name: str,
        instance_name: str,
        resource_group_name: str,
        confirm_yes: bool = False,
        **kwargs
    ) -> dict:
        should_bail = not should_continue_prompt(
            confirm_yes=confirm_yes,
        )
        if should_bail:
            return

        with console.status("Working..."):
            poller = self.ops.begin_delete(
                dataflow_profile_name=dataflow_profile_name,
                dataflow_name=name,
                instance_name=instance_name,
                resource_group_name=resource_group_name,
            )
            return wait_for_terminal_state(poller, **kwargs)
        
    def _validate_dataflow_config(
        self,
        dataflow_config: dict,
        instance_name: str,
        resource_group_name: str,
    ):
        operations = dataflow_config.get("operations", [])
        # # get source endpoint
        # source_operation = next(
        #     (op for op in operations if op.get("operationType") == "Source"), {}
        # )
        # source_endpoint_name = source_operation.get("sourceSettings", {}).get("endpointRef", "")
        # # call get_dataflow_endpoint
        # dataflow_endpoint = DataFlowEndpoints(self.cmd)
        # source_endpoint_obj = dataflow_endpoint.show(
        #     name=source_endpoint_name,
        #     instance_name=instance_name,
        #     resource_group_name=resource_group_name,
        # )

        # if not source_endpoint_obj:
        #     raise InvalidArgumentValueError(
        #         f"Source dataflow endpoint '{source_endpoint_name}' not found in instance '{instance_name}'"
        #     )
        # get source endpoint
        source_endpoint_obj = self._process_exist_endpoint(
            operations=operations,
            instance_name=instance_name,
            resource_group_name=resource_group_name,
            operation_type=DataflowOperationType.SOURCE.value,
        )
        
        # validate source endpoint type
        source_endpoint_type = source_endpoint_obj.get("properties", {}).get("endpointType", "")
        if source_endpoint_type not in [
            DataflowEndpointType.KAFKA.value,
            DataflowEndpointType.MQTT.value,
        ]:
            raise InvalidArgumentValueError(
                f"Source dataflow endpoint '{source_endpoint_type}' is not a valid type for dataflow."
            )
        
        # when Kafka endpoint, validate consumer group id
        if source_endpoint_type == DataflowEndpointType.KAFKA.value:
            group_id = source_endpoint_obj.get("properties", {}).get("kafkaSettings", {}).get("consumerGroupId", "")
            if not group_id:
                raise InvalidArgumentValueError(
                    f"Consumer group id is required for source dataflow endpoint."
                )
        
        
        # get destination endpoint
        desination_endpoint_obj = self._process_exist_endpoint(
            operations=operations,
            instance_name=instance_name,
            resource_group_name=resource_group_name,
            operation_type=DataflowOperationType.DESTINATION.value,
        )
        # destination_operation = next(
        #     (op for op in operations if op.get("operationType") == "Destination"), {}
        # )
        # destination_endpoint_name = destination_operation.get("destinationSettings", {}).get("endpointRef", "")
        # # call get_dataflow_endpoint
        # desination_endpoint_obj = dataflow_endpoint.show(
        #     name=destination_endpoint_name,
        #     instance_name=instance_name,
        #     resource_group_name=resource_group_name,
        # )
        # if not desination_endpoint_obj:
        #     raise InvalidArgumentValueError(
        #         f"Destination dataflow endpoint '{destination_endpoint_name}' not found in instance '{instance_name}'"
        #     )
        
        transformation_operation = next(
            (op for op in operations if op.get("operationType") == "BuiltInTransformation"), {}
        )
        schema_ref = transformation_operation.get("builtInTransformationSettings", {}).get("schemaRef", "")
        
        # validate schema_ref for destination endpoint type
        destination_endpoint_type = desination_endpoint_obj.get("properties", {}).get("endpointType", "")
        if destination_endpoint_type not in [
            DataflowEndpointType.DATAEXPLORER.value,
            DataflowEndpointType.DATALAKESTORAGE.value,
            DataflowEndpointType.FABRICONELAKE.value,
            DataflowEndpointType.LOCALSTORAGE.value,
        ] and not schema_ref:
            raise InvalidArgumentValueError(
                f"'schemaRef' is required for dataflow due to destination endpoint type '{destination_endpoint_type}'"
            )
        
        # validate one of source and destination endpoint must be MQTT endpoint
        if source_endpoint_type != DataflowEndpointType.MQTT.value and destination_endpoint_type != DataflowEndpointType.MQTT.value:
            raise InvalidArgumentValueError(
                f"Either source or destination endpoint must be MQTT endpoint."
            )

        # validate one of source and destination endpoint must have host with "aio-broker"
        source_endpoint_host = source_endpoint_obj.get("properties", {}).get(DATAFLOW_ENDPOINT_TYPE_SETTINGS[source_endpoint_type], {}).get("host", "")
        destination_endpoint_host = desination_endpoint_obj.get("properties", {}).get(DATAFLOW_ENDPOINT_TYPE_SETTINGS[source_endpoint_type], {}).get("host", "")
        if "aio-broker" not in source_endpoint_host and "aio-broker" not in destination_endpoint_host:
            raise InvalidArgumentValueError(
                f"Either source or destination endpoint must have host with 'aio-broker'."
            )
        
    def _process_exist_endpoint(
        self,
        operations: list,
        instance_name: str,
        resource_group_name: str,
        operation_type: str,
    ) -> dict:
        # get endpoint
        operation = next(
            (op for op in operations if op.get("operationType") == operation_type), {}
        )

        # get operation settings
        if operation_type == "Source":
            operation_settings = operation.get("sourceSettings", {})
        elif operation_type == "Destination":
            operation_settings = operation.get("destinationSettings", {})
        elif operation_type == "BuiltInTransformation":
            operation_settings = operation.get("builtInTransformationSettings", {})
        endpoint_name = operation_settings.get("endpointRef", "")
        # call get_dataflow_endpoint
        dataflow_endpoint = DataFlowEndpoints(self.cmd)
        endpoint_obj = dataflow_endpoint.show(
            name=endpoint_name,
            instance_name=instance_name,
            resource_group_name=resource_group_name,
        )

        if not endpoint_obj:
            raise InvalidArgumentValueError(
                f"{operation_type} dataflow endpoint '{endpoint_name}' not found in instance '{instance_name}'. "
                "Please provide a valid 'endpointRef' using --config-file."
            )
        
        return endpoint_obj


class DataFlowEndpoints(Queryable):
    def __init__(self, cmd):
        super().__init__(cmd=cmd)
        self.iotops_mgmt_client = get_iotops_mgmt_client(
            subscription_id=self.default_subscription_id,
        )
        self.ops: "DataflowEndpointOperations" = self.iotops_mgmt_client.dataflow_endpoint
        self.instances = Instances(self.cmd)
    
    def create(
        self,
        name: str,
        instance_name: str,
        resource_group_name: str,
        endpoint_type: DataflowEndpointType,
        **kwargs
    ) -> dict:
        self.instance = self.instances.show(name=instance_name, resource_group_name=resource_group_name)
        extended_location = self.instance["extendedLocation"]
        settings = {}

        self._process_authentication_type(
            endpoint_type=endpoint_type,
            authentication_method=kwargs.get("authentication_method"),
            settings=settings,
            **kwargs
        )

        self._process_endpoint_properties(
            endpoint_type=endpoint_type,
            settings=settings,
            **kwargs
        )

        resource = {
            "extendedLocation": extended_location,
            "properties": {
                "endpointType": endpoint_type.value,
                DATAFLOW_ENDPOINT_TYPE_SETTINGS[endpoint_type.value]: settings,
            }
        }

        return self.ops.begin_create_or_update(
            resource_group_name=resource_group_name,
            instance_name=instance_name,
            dataflow_endpoint_name=name,
            resource=resource,
        )

    def show(self, name: str, instance_name: str, resource_group_name: str) -> dict:
        return self.ops.get(
            resource_group_name=resource_group_name,
            instance_name=instance_name,
            dataflow_endpoint_name=name,
        )

    def list(self, instance_name: str, resource_group_name: str) -> Iterable[dict]:
        return self.ops.list_by_resource_group(resource_group_name=resource_group_name, instance_name=instance_name)


    def _process_authentication_type(
        self,
        endpoint_type: DataflowEndpointType,
        authentication_method: DataflowEndpointAuthenticationType,
        settings: dict,
        **kwargs
    ):
        # No authentication method required for local storage
        if endpoint_type == DataflowEndpointType.LOCALSTORAGE.value:
            return
        
        # Check if authentication method is allowed for the given endpoint type
        if authentication_method not in DATAFLOW_ENDPOINT_AUTHENTICATION_TYPE_MAP[endpoint_type.value]:
            raise ValueError(
                f"Authentication method '{authentication_method}' is not allowed for endpoint type '{endpoint_type}'. "
                f"Allowed methods are: {DATAFLOW_ENDPOINT_AUTHENTICATION_TYPE_MAP[endpoint_type.value]}"
            )
        
        # Check required properties for authentication method
        required_params = AUTHENTICATION_TYPE_REQUIRED_PARAMS.get(authentication_method, [])
        missing_params = [param for param in required_params if param not in kwargs]

        if missing_params:
            raise ValueError(
                f"Missing required parameters for authentication method '{authentication_method}': {', '.join(missing_params)}"
            )
        
        settings["authentication"] = {
            "method": authentication_method,
        }

        if authentication_method == DataflowEndpointAuthenticationType.ANONYMOUS.value:
            return

        auth_settings = {}
        for param_name, property_name in [
            ("audience", "audience"),
            ("client_id", "clientId"),
            ("tenant_id", "tenantId"),
            ("scope", "scope"),
            ("secret_name", "secretRef"),
            ("sasl_type", "saslType"),
        ]:
            if kwargs.get(param_name):
                auth_settings[property_name] = kwargs[param_name]

        settings["authentication"][DataflowEndpointAuthenticationType.ANONYMOUS.value+"Settings"] = auth_settings
        
        return
    

    def _process_endpoint_properties(
        self,
        endpoint_type: DataflowEndpointType,
        settings: dict,
        **kwargs
    ):
        # # Check required properties for endpoint type
        # required_params = DATAFLOW_ENDPOINT_TYPE_REQUIRED_PARAMS.get(endpoint_type.value, [])
        # missing_params = [param for param in required_params if param not in kwargs]

        # if missing_params:
        #     raise ValueError(
        #         f"Missing required parameters for endpoint type '{endpoint_type}': {', '.join(missing_params)}"
        #     )
        
        if kwargs.get("database_name"):
            settings["database"] = kwargs["database_name"]
        if kwargs.get("host"):
            settings["host"] = kwargs["host"]
        if kwargs.get("batching_latency") or kwargs.get("message_count"):
            settings["batching"] = {}
            if kwargs.get("batching_latency"):
                settings["batching"]["latencySeconds"] = kwargs["batching_latency"]
            if kwargs.get("message_count"):
                settings["batching"]["maxMessages"] = kwargs["message_count"]
        if kwargs.get("lakehouse_name") or kwargs.get("workspace_name"):
            settings["names"] = {}
            if kwargs.get("lakehouse_name"):
                settings["names"]["lakehouseName"] = kwargs["lakehouse_name"]
            if kwargs.get("workspace_name"):
                settings["names"]["workspaceName"] = kwargs["workspace_name"]
        if kwargs.get("path_type"):
            settings["oneLakePathType"] = kwargs["path_type"]
        if kwargs.get("group_id"):
            settings["consumerGroupId"] = kwargs["group_id"]
        if kwargs.get("copy_broker_props_disabled"):
            settings["copyMqttProperties"] = not kwargs["copy_broker_props_disabled"]
        if kwargs.get("compression"):
            settings["compression"] = kwargs["compression"]
        if kwargs.get("aks"):
            settings["aks"] = kwargs["aks"]
        if kwargs.get("patition_strategy"):
            settings["partitionStrategy"] = kwargs["patition_strategy"]
        if kwargs.get("tls_disabled") or kwargs.get("tls_config_map_reference"):
            settings["tls"] = {}
            if kwargs.get("tls_disabled"):
                settings["tls"]["mode"] = not kwargs["tls_disabled"]
            if kwargs.get("tls_config_map_reference"):
                settings["tls"]["configMapRef"] = kwargs["tls_config_map_reference"]
        if kwargs.get("cloud_event_attribute"):
            settings["cloudEventAttributes"] = kwargs["cloud_event_attribute"]
        if kwargs.get("pvc_reference"):
            settings["persistentVolumeClaimRef"] = kwargs["pvc_reference"]
        if kwargs.get("client_id_prefix"):
            settings["clientIdPrefix"] = kwargs["client_id_prefix"]
        if kwargs.get("protocol"):
            settings["protocol"] = kwargs["protocol"]
        if kwargs.get("keep_alive"):
            settings["keepAliveSeconds"] = kwargs["keep_alive"]
        if kwargs.get("retain"):
            settings["retain"] = kwargs["retain"]
        if kwargs.get("max_inflight_messages"):
            settings["maxInflightMessages"] = kwargs["max_inflight_messages"]
        if kwargs.get("qos"):
            settings["qos"] = kwargs["qos"]
        if kwargs.get("session_expiry"):
            settings["sessionExpirySeconds"] = kwargs["session_expiry"]
        
        return
