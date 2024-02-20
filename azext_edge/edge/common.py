# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

"""
shared: Define shared data types(enums) and constant strings.

"""

from enum import Enum


class ListableEnum(Enum):
    @classmethod
    def list(cls):
        return [c.value for c in cls]


class CheckTaskStatus(Enum):
    """
    Status of a check task.
    """

    success = "success"
    warning = "warning"
    error = "error"
    skipped = "skipped"


class ResourceState(Enum):
    """
    K8s resource state.
    """

    starting = "Starting"
    running = "Running"
    recovering = "Recovering"
    failed = "Failed"
    ok = "OK"
    warn = "warn"
    error = "Error"


class PodState(Enum):
    """
    K8s pod state.
    """

    pending = "Pending"
    running = "Running"
    succeeded = "Succeeded"
    failed = "Failed"
    unknown = "Unknown"


class ProvisioningState(Enum):
    """
    Resource provisioning state.
    """

    succeeded = "Succeeded"
    failed = "Failed"
    updating = "Updating"
    canceled = "Canceled"
    provisioning = "Provisioning"
    deleting = "Deleting"
    accepted = "Accepted"


class MqDiagnosticPropertyIndex(Enum):
    """
    MQ Diagnostic Property Index Strings
    """

    publishes_received_per_second = "aio_mq_publishes_received_per_second"
    publishes_sent_per_second = "aio_mq_publishes_sent_per_second"
    publish_route_replication_correctness = "aio_mq_publish_route_replication_correctness"
    publish_latency_mu_ms = "aio_mq_publish_latency_mu_ms"
    publish_latency_sigma_ms = "aio_mq_publish_latency_sigma_ms"
    connected_sessions = "aio_mq_connected_sessions"
    total_subscriptions = "aio_mq_total_subscriptions"


class OpsServiceType(ListableEnum):
    """
    IoT Operations service type.
    """

    auto = "auto"
    mq = "mq"
    lnm = "lnm"
    opcua = "opcua"
    dataprocessor = "dataprocessor"
    orc = "orc"
    akri = "akri"
    deviceregistry = "deviceregistry"


class ResourceTypeMapping(Enum):
    """
    Resource type mappings for graph queries.
    """

    asset = "Microsoft.DeviceRegistry/assets"
    asset_endpoint_profile = "Microsoft.DeviceRegistry/assetEndpointProfiles"
    custom_location = "Microsoft.ExtendedLocation/customLocations"
    connected_cluster = "Microsoft.Kubernetes/connectedClusters"
    cluster_extensions = "Microsoft.KubernetesConfiguration/extensions"
    # TODO: update dataset resource type
    dataset = "Microsoft.IoTOperationsDataProcessor/datasets"
    instance = "Microsoft.IoTOperationsDataProcessor/instances"
    pipeline = "Microsoft.IoTOperationsDataProcessor/instances/pipelines"


class ClusterExtensionsMapping(Enum):
    """
    Cluster extension mappings.
    """

    asset = "microsoft.deviceregistry.assets"
    dataprocessor = "microsoft.iotoperations.dataprocessor"


class AEPAuthModes(Enum):
    """
    Authentication modes for asset endpoints
    """
    anonymous = "Anonymous"
    certificate = "Certificate"
    userpass = "UsernamePassword"


class DPPipelineStageAuthModes(Enum):
    """
    Authentication modes for data processor pipeline mqtt stages
    """
    none = "none"
    usernamePassword = "usernamePassword"
    serviceAccountToken = "serviceAccountToken"


class DPPipelineInputStageTypes(Enum):
    """
    Input stage types for data processor pipeline
    """
    http = "input/http@v1"
    influxdb = "input/influxdbv2@v1"
    mqtt = "input/mqtt@v1"
    sql = "input/sqlserver@v1"


class DPPipelineOutputStageTypes(Enum):
    """
    Output stage types for data processor pipeline
    """
    blobStorage = "output/blobstorage@v1"
    dataExplorer = "output/dataexplorer@v1"
    fabric = "output/fabric@v1"
    file = "output/file@v1"
    grpc = "output/grpc@v1"
    http = "output/http@v1"
    mqtt = "output/mqtt@v1"
    referenceData = "output/refdata@v1"


class DPPipelineProcessorStageTypes(Enum):
    """
    Processor stage types for data processor pipeline
    """
    aggregate = "processor/aggregate@v1"
    enrich = "processor/enrich@v1"
    filter = "processor/filter@v1"
    grpc = "processor/grpc@v1"
    http = "processor/http@v1"
    lkv = "processor/lkv@v1"
    transform = "processor/transform@v1"


class K8sSecretType(Enum):
    """
    Supported k8s secret types.
    """

    opaque = "Opaque"
    tls = "kubernetes.io/tls"


# MQ runtime attributes

AIO_MQ_DIAGNOSTICS_SERVICE = "aio-mq-diagnostics-service"
AIO_MQ_OPERATOR = "aio-mq-operator"
METRICS_SERVICE_API_PORT = 9600
PROTOBUF_SERVICE_API_PORT = 9800
