# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

"""
Load CLI commands
"""
from azure.cli.core.commands import CliCommandType

mq_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_mq#{}")
edge_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_edge#{}")
asset_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_assets#{}")
aep_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_asset_endpoint_profiles#{}")
dataset_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_datasets#{}")
instance_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_instances#{}")
pipeline_resource_ops = CliCommandType(operations_tmpl="azext_edge.edge.commands_pipelines#{}")


def load_iotops_commands(self, _):
    """
    Load CLI commands
    """
    with self.command_group(
        "iot ops",
        command_type=edge_resource_ops,
        is_preview=True,
    ) as cmd_group:
        cmd_group.command("check", "check")
        cmd_group.command("init", "init")

    with self.command_group(
        "iot ops support",
        command_type=edge_resource_ops,
    ) as cmd_group:
        cmd_group.command("create-bundle", "support_bundle")

    with self.command_group(
        "iot ops mq",
        command_type=mq_resource_ops,
    ) as cmd_group:
        cmd_group.command("stats", "stats")
        cmd_group.command("get-password-hash", "get_password_hash")

    with self.command_group(
        "iot ops asset",
        command_type=asset_resource_ops,
    ) as cmd_group:
        cmd_group.command("create", "create_asset")
        cmd_group.command("delete", "delete_asset")
        cmd_group.command("query", "query_assets")
        cmd_group.show_command("show", "show_asset")
        cmd_group.command("update", "update_asset")

    with self.command_group(
        "iot ops asset data-point",
        command_type=asset_resource_ops,
    ) as cmd_group:
        cmd_group.command("add", "add_asset_data_point")
        cmd_group.command("list", "list_asset_data_points")
        cmd_group.command("remove", "remove_asset_data_point")

    with self.command_group(
        "iot ops asset event",
        command_type=asset_resource_ops,
    ) as cmd_group:
        cmd_group.command("add", "add_asset_event")
        cmd_group.command("list", "list_asset_events")
        cmd_group.command("remove", "remove_asset_event")

    with self.command_group(
        "iot ops asset endpoint",
        command_type=aep_resource_ops,
    ) as cmd_group:
        cmd_group.command("create", "create_asset_endpoint_profile")
        cmd_group.command("delete", "delete_asset_endpoint_profile")
        cmd_group.command("query", "query_asset_endpoint_profiles")
        cmd_group.show_command("show", "show_asset_endpoint_profile")
        cmd_group.command("update", "update_asset_endpoint_profile")

    with self.command_group(
        "iot ops asset endpoint certificate",
        command_type=aep_resource_ops,
    ) as cmd_group:
        cmd_group.command("add", "add_asset_endpoint_profile_transport_auth")
        cmd_group.command("list", "list_asset_endpoint_profile_transport_auth")
        cmd_group.command("remove", "remove_asset_endpoint_profile_transport_auth")

    with self.command_group(
        "iot ops dataprocessor dataset",
        command_type=dataset_resource_ops,
    ) as cmd_group:
        cmd_group.command("create", "create_dataset")
        cmd_group.command("show", "show_dataset")
        cmd_group.command("list", "list_datasets")
        cmd_group.command("delete", "delete_dataset")
        cmd_group.command("query", "query_datasets")
    
    with self.command_group(
        "iot ops dataprocessor instance",
        command_type=instance_resource_ops,
    ) as cmd_group:
        cmd_group.command("list", "list_instances")
        cmd_group.command("show", "show_instance")
        cmd_group.command("query", "query_instances")
    
    with self.command_group(
        "iot ops dataprocessor pipeline",
        command_type=pipeline_resource_ops,
    ) as cmd_group:
        cmd_group.command("create", "create_pipeline")

    with self.command_group(
        "iot ops dp pipeline source add",
        command_type=pipeline_resource_ops,
    ) as cmd_group:
        cmd_group.command("mqtt", "add_pipeline_source_mqtt")
        cmd_group.command("http", "add_pipeline_source_mqtt")
        cmd_group.command("influxdb", "add_pipeline_source_mqtt")
        cmd_group.command("sql", "add_pipeline_source_mqtt")
    
    with self.command_group(
        "iot ops dp pipeline destination add",
        command_type=pipeline_resource_ops,
    ) as cmd_group:
        cmd_group.command("mqtt", "add_pipeline_destination_mqtt")
    
    with self.command_group(
        "iot ops dp pipeline processor add",
        command_type=pipeline_resource_ops,
    ) as cmd_group:
        cmd_group.command("enrich", "add_pipeline_processor_enrich")
