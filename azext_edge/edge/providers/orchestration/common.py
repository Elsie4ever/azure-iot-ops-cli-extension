# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from enum import Enum
from ...util.x509 import DEFAULT_VALID_DAYS as DEFAULT_X509_CA_VALID_DAYS


# Urls
ARM_ENDPOINT = "https://management.azure.com/"
MCR_ENDPOINT = "https://mcr.microsoft.com/"
GRAPH_ENDPOINT = "https://graph.microsoft.com/"
GRAPH_V1_ENDPOINT = f"{GRAPH_ENDPOINT}v1.0"
GRAPH_V1_SP_ENDPOINT = f"{GRAPH_V1_ENDPOINT}/servicePrincipals"

CUSTOM_LOCATIONS_RP_APP_ID = "bc313c14-388c-4e7d-a58e-70017303ee3b"

EXTENDED_LOCATION_ROLE_BINDING = "AzureArc-Microsoft.ExtendedLocation-RP-RoleBinding"
ARC_CONFIG_MAP = "azure-clusterconfig"
ARC_NAMESPACE = "azure-arc"

# Key Vault KPIs
KEYVAULT_DATAPLANE_API_VERSION = "7.4"  # TODO - @digimaun, maybe needed
KEYVAULT_CLOUD_API_VERSION = "2022-07-01"  # TODO - @digimaun, maybe needed

# Custom Locations KPIs
CUSTOM_LOCATIONS_API_VERSION = "2021-08-31-preview"

AIO_INSECURE_LISTENER_NAME = "default-insecure"
AIO_INSECURE_LISTENER_SERVICE_NAME = "aio-broker-insecure"
AIO_INSECURE_LISTENER_SERVICE_PORT = 1883


class MqMode(Enum):
    auto = "auto"
    distributed = "distributed"


class MqMemoryProfile(Enum):
    tiny = "Tiny"
    low = "Low"
    medium = "Medium"
    high = "High"


class MqServiceType(Enum):
    cluster_ip = "ClusterIp"
    load_balancer = "LoadBalancer"
    node_port = "NodePort"


class KubernetesDistroType(Enum):
    k3s = "K3s"
    k8s = "K8s"
    microk8s = "MicroK8s"


class TrustSourceType(Enum):
    self_signed = "SelfSigned"
    # customer_managed = "CustomerManaged"  # TODO - @digimaun


__all__ = [
    "MqMode",
    "MqMemoryProfile",
    "MqServiceType",
    "KubernetesDistroType",
    "TrustSourceType",
    "DEFAULT_X509_CA_VALID_DAYS",
    "KEYVAULT_DATAPLANE_API_VERSION",
    "KEYVAULT_CLOUD_API_VERSION",
]
