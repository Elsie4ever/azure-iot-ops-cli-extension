# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from knack.log import get_logger
from typing import Any, Dict, List, Optional

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
                            "brokerlisteners.mq.iotoperations.azure.com": [
                                {
                                    "namespace" : "_all_",
                                    "conditions": [
                                        "len(brokerlisteners)>=1",
                                    ],
                                    "evaluations": [
                                        {
                                            "name": "brokerlisteners",
                                            "value": {
                                                "len(brokerlisteners)": 1,
                                            },
                                            "status": "success"
                                        }
                                    ],
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
                                {
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
                                            "status": "warning"
                                        }
                                    ],
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
                            ] 
                        }
                    }
                ],
            },
            "akri": {...}
        ],

    """
