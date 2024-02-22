# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

import os
import json
from typing import Any, Dict, Tuple
from azext_edge.edge.util.common import build_query
from knack.log import get_logger
from msrest.serialization import Model
from pathlib import Path

from ..base_provider import RPSaaSBaseProvider
from ....common import ClusterExtensionsMapping, ResourceTypeMapping

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
    

    def get_dp_instance_name(
            self,
            extended_location: Dict[str, str] ) -> str:
        custom_query = "| where extendedLocation.name contains \"{}\"".format(extended_location["name"])
        instance_name = build_query(
            self.cmd,
            subscription_id=self.subscription,
            type=ResourceTypeMapping.instance.value,
            custom_query=custom_query,
        )
        # return the first instance name
        return instance_name[0]["name"]


class MicroObjectCache(object):
    def __init__(self, cmd, models):
        from azure.cli.core.commands.client_factory import get_subscription_id
        from msrest import Serializer, Deserializer

        client_models = {k: v for k, v in models.__dict__.items() if isinstance(v, type)}
        self._serializer = Serializer(client_models)
        self._deserializer = Deserializer(client_models)

        self.cmd = cmd
        self.subscription_id: str = get_subscription_id(self.cmd.cli_ctx)
        if not self.subscription_id:
            raise RuntimeError("Unable to determine subscription Id.")
        self.cloud_name: str = self.cmd.cli_ctx.cloud.name

    def set(
        self, resource_name: str, resource_group: str, resource_type: str, payload: Model, serialization_model: str
    ):
        saved_input = self._save(
            resource_name=resource_name,
            resource_group=resource_group,
            resource_type=resource_type,
            payload=self._serializer.body(payload, serialization_model),
        )
        return json.loads(saved_input)["_payload"]

    def get(self, resource_name: str, resource_group: str, resource_type: str, serialization_model: str) -> Any:
        return self._load(
            resource_name=resource_name,
            resource_group=resource_group,
            resource_type=resource_type,
            serialization_model=serialization_model,
        )

    @classmethod
    def get_config_dir(cls) -> str:
        return os.getenv("AZURE_CONFIG_DIR") or os.path.expanduser(os.path.join("~", ".azure"))

    def _get_file_path(self, resource_name: str, resource_group: str, resource_type: str) -> Tuple[str, str]:
        directory = os.path.join(
            self.get_config_dir(),
            "object_cache",
            self.cloud_name,
            self.subscription_id,
            resource_group,
            resource_type,
        )
        filename = "{}.json".format(resource_name)
        return directory, filename

    def _save(self, resource_name: str, resource_group: str, resource_type: str, payload: Any):
        from datetime import datetime
        from knack.util import ensure_dir

        directory, filename = self._get_file_path(
            resource_name=resource_name, resource_group=resource_group, resource_type=resource_type
        )
        ensure_dir(directory)
        target_path = Path(os.path.join(directory, filename))
        with open(str(target_path), mode="w", encoding="utf8") as f:
            logger.info("Caching '%s' to: '%s'", resource_name, str(target_path))
            cache_obj_dump = json.dumps({"last_saved": str(datetime.now()), "_payload": payload})
            f.write(cache_obj_dump)
        
        return cache_obj_dump

    def _load(self, resource_name: str, resource_group: str, resource_type: str, serialization_model: str) -> Any:
        directory, filename = self._get_file_path(
            resource_name=resource_name, resource_group=resource_group, resource_type=resource_type
        )
        target_path = Path(os.path.join(directory, filename))
        if target_path.exists():
            with open(str(target_path), mode="r", encoding="utf8") as f:
                logger.info(
                    "Loading '%s' from cache: %s",
                    resource_name,
                    str(target_path),
                )
                obj_data = json.loads(f.read())
                if "_payload" in obj_data:
                    return self._deserializer.deserialize_data(obj_data["_payload"], serialization_model)

    def remove(self, resource_name: str, resource_group: str, resource_type: str):
        directory, filename = self._get_file_path(
            resource_name=resource_name, resource_group=resource_group, resource_type=resource_type
        )
        try:
            target_path = Path(os.path.join(directory, filename))
            if target_path.exists():
                os.remove(str(target_path))
        except (OSError, IOError):
            pass
