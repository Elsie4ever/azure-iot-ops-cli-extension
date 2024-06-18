# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from knack.log import get_logger
from typing import Any, Dict, List, Optional

from azext_edge.edge.common import CheckTaskStatus
from azext_edge.edge.providers.check.common import ALL_NAMESPACES_TARGET

logger = get_logger(__name__)

class CheckSchemaProperty:
    def __init__(self, type_: str, display_text: str, should_check: bool):
        self.type = type_
        self.display_text = display_text
        self.should_check = should_check

    def __repr__(self):
        return f"CheckSchemaProperty(type={self.type}, display_text='{self.display_text}', should_check={self.should_check})"

class CheckSchema:
    """
        {
            "summary": [
                "property1": {
                    "type": "string",
                    "displayText": "Text to display for property1",
                    "shouldCheck": true,
                },
                "property2": {
                    "type": "string",
                    "displayText": "Text to display for property2",
                    "shouldCheck": true,
                },
            ],
            "simplified": [
                "property1": {
                    "type": "string",
                    "displayText": "Text to display for property1",
                    "shouldCheck": true,
                },
                "property2": {
                    "type": "string",
                    "displayText": "Text to display for property2",
                    "shouldCheck": true,
                },
                ...
            ],
            "expanded": [
                "property.property1": {
                    "type": "string",
                    "displayText": "Text to display for property.property1",
                    "shouldCheck": true,
                },
                "property2": {
                    "type": "string",
                    "displayText": "Text to display for property2",
                    "shouldCheck": true,
                },
                "property3": {
                    "type": "Integer",
                    "displayText": "Text to display for property3",
                    "shouldCheck": False, # This property will not be checked, display only
                },
                ...
            ]
        }
    """
    def __init__(self, data: Dict[str, Any]):
        self.summary = self._parse_properties(data.get("summary", {}))
        self.simplified = self._parse_properties(data.get("simplified", {}))
        self.expanded = self._parse_properties(data.get("expanded", {}))

    @staticmethod
    def _parse_properties(properties: Dict[str, Dict[str, Any]]) -> Dict[str, CheckSchemaProperty]:
        parsed_properties = {}
        for key, value in properties.items():
            parsed_properties[key] = CheckSchemaProperty(
                type_=value["type"],
                display_text=value["displayText"],
                should_check=value.get("shouldCheck", False)
            )
        return parsed_properties

    def __repr__(self):
        return f"DataStructure(summary={self.summary}, simplified={self.simplified}, expanded={self.expanded})"
    

class CheckManager:
    """
        [
            "mq": {
                "preDeployment": [
                    {
                        "description": "Evaluate Kubernetes server",
                        "name": "evalK8sVers",
                        "status": "success",
                        "targets": {
                            "k8s": [
                                "namespace": "_all_",
                                "conditions": [
                                    "(k8s version)>=1.20"
                                ],
                                "evaluations": [
                                    {
                                        "status": "success",
                                        "value": "1.25"
                                    }
                                ],
                                "status": "success"
                            ]
                        }
                    },
                    ...
                ],
                "postDeployment": [
                    {
                        "name": "evaluateBrokerListeners",
                        "description": "Evaluate MQ broker listeners",
                        "status": "warning",
                        "targets": {
                            "brokerlisteners.mq.iotoperations.azure.com": {
                                description: "Text to display for brokerlisteners.mq.iotoperations.azure.com",
                                conditions: [
                                    "len(brokerlisteners)>=1",
                                ],
                                "evaluations": [
                                    {
                                        "name": "brokerlisteners",
                                        "value": {
                                            "len(brokerlisteners)": 1,
                                        },
                                        "status": "success"
                                    },
                                ],
                                "status": "success",
                                items: [
                                    {
                                        "name": "brokerlistener1",
                                        "namespace": "default",
                                        "conditions": [
                                            "valid(spec.brokerRef)",
                                            "spec.serviceName",
                                            "status"
                                        ],
                                        "evaluations": [
                                            {
                                                "spec": {
                                                    "serviceName": "aio-mq-dmqtt-frontend",
                                                },
                                                "valid(spec.brokerRef)": true,
                                                "status": "success"
                                            }
                                        ],
                                        "status": "success",
                                        # these are not checked but displayed
                                        "additional_properties": [
                                            {
                                                name: "property1",
                                                display_text: "Text to display for property1",
                                                "value": "value1"
                                            },
                                            {
                                                name: "property2",
                                                display_text: "Text to display for property2",
                                                "value": "value2"
                                            },
                                        ]
                                    },
                                    ...
                                ]
                            }
                        }
                    }
                ],
            },
            "akri": {...}
        ],

    """
    def __init__(self, check_name: str, check_desc: str, check_schema: Optional[dict] = None):
        self.check_name = check_name
        self.check_desc = check_desc
        self.targets = {}
        self.target_displays = {}
        self.worst_status = CheckTaskStatus.success.value
        self.check_schema = CheckSchema(check_schema) if check_schema else None
    

    def add_target(
        self,
        target_name: str,
        conditions: List[str] = None,
        description: str = None,
    ) -> None:
        # TODO: maybe make a singular taget into a class for consistent structure?
        if target_name not in self.targets:
            # Create a default `None` namespace target for targets with no namespace
            self.targets[target_name] = {}
        self.targets[target_name]["conditions"] = conditions
        self.targets[target_name]["evaluations"] = []
        self.targets[target_name]["status"] = CheckTaskStatus.success.value
        self.targets[target_name]["items"] = []
        if description:
            self.targets[target_name]["description"] = description

    def set_target_conditions_to_items(
        self, target_name: str, source_name: str, conditions: List[str], namespace: str
    ) -> None:
        for item in self.targets[target_name]["items"]:
            if item["name"] == source_name and item["namespace"] == namespace:
                item["conditions"] = conditions
            else:
                self.targets[target_name]["items"] = {
                    "name": source_name,
                    "namespace": namespace,
                    "conditions": conditions,
                }
        self.targets[target_name][namespace]["conditions"] = conditions

    def add_target_conditions(
        self, target_name: str, conditions: List[str], namespace: str = ALL_NAMESPACES_TARGET
    ) -> None:
        if self.targets[target_name][namespace]["conditions"] is None:
            self.targets[target_name][namespace]["conditions"] = []
        self.targets[target_name][namespace]["conditions"].extend(conditions)

    def set_target_status(self, target_name: str, status: str, namespace: str = ALL_NAMESPACES_TARGET) -> None:
        self._process_status(target_name=target_name, namespace=namespace, status=status)

    def add_target_eval(
        self,
        target_name: str,
        status: str,
        value: Optional[Any] = None,
        namespace: str = ALL_NAMESPACES_TARGET,
        resource_name: Optional[str] = None,
        resource_kind: Optional[str] = None,
    ) -> None:
        eval_dict = {"status": status}
        if resource_name:
            eval_dict["name"] = resource_name
        if value:
            eval_dict["value"] = value
        if resource_kind:
            eval_dict["kind"] = resource_kind
        self.targets[target_name][namespace]["evaluations"].append(eval_dict)
        self._process_status(target_name, status, namespace)

    def _process_status(self, target_name: str, status: str, namespace: str = ALL_NAMESPACES_TARGET) -> None:
        existing_status = self.targets[target_name].get("status", CheckTaskStatus.success.value)
        if existing_status != status:
            if existing_status == CheckTaskStatus.success.value and status in [
                CheckTaskStatus.warning.value,
                CheckTaskStatus.error.value,
                CheckTaskStatus.skipped.value,
            ]:
                self.targets[target_name][namespace]["status"] = status
                self.worst_status = status
            elif existing_status in [
                CheckTaskStatus.warning.value, CheckTaskStatus.skipped.value
            ] and status in [CheckTaskStatus.error.value]:
                self.targets[target_name][namespace]["status"] = status
                self.worst_status = status

    def add_display(self, target_name: str, display: Any, namespace: str = ALL_NAMESPACES_TARGET) -> None:
        if target_name not in self.target_displays:
            self.target_displays[target_name] = {}
        if namespace not in self.target_displays[target_name]:
            self.target_displays[target_name][namespace] = []
        self.target_displays[target_name][namespace].append(display)

    def as_dict(self, as_list: bool = False) -> Dict[str, Any]:
        from copy import deepcopy

        result = {
            "name": self.check_name,
            "description": self.check_desc,
            "targets": {},
            "status": self.worst_status,
        }
        result["targets"] = deepcopy(self.targets)
        if as_list:
            for type in self.target_displays:
                for namespace in self.target_displays[type]:
                    result["targets"][type][namespace]["displays"] = deepcopy(self.target_displays[type][namespace])

        return result
