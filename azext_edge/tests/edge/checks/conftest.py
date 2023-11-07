# coding=utf-8
# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Private distribution for NDA customers only. Governed by license terms at https://preview.e4k.dev/docs/use-terms/
# --------------------------------------------------------------------------------------------
from typing import List, Dict, Any
import pytest

from azext_edge.edge.providers.checks import run_checks


@pytest.fixture
def mock_evaluate_mq_pod_health(mocker):
    patched = mocker.patch("azext_edge.edge.providers.check.mq.evaluate_pod_health", return_value={})
    yield patched


@pytest.fixture
def mock_evaluate_cloud_connector_pod_health(mocker):
    patched = mocker.patch("azext_edge.edge.providers.check.cloud_connectors.evaluate_pod_health", return_value={})
    yield patched


@pytest.fixture
def mock_evaluate_dataprocessor_pod_health(mocker):
    patched = mocker.patch("azext_edge.edge.providers.check.dataprocessor.evaluate_pod_health", return_value={})
    yield patched


@pytest.fixture
def mock_evaluate_lnm_pod_health(mocker):
    patched = mocker.patch("azext_edge.edge.providers.check.lnm._evaluate_lnm_pod_health", return_value={})
    yield patched


@pytest.fixture
def mock_get_namespaced_pods_by_prefix(mocker):
    patched = mocker.patch("azext_edge.edge.providers.check.lnm.get_namespaced_pods_by_prefix", return_value=[])
    yield patched


@pytest.fixture
def mock_resource_types(mocker, ops_service):
    patched = mocker.patch("azext_edge.edge.providers.check.base.enumerate_ops_service_resources")

    if ops_service == "mq":
        patched.return_value = (
            {},
            {
                "Broker": [{}],
                "BrokerListener": [{}],
                "DiagnosticService": [{}],
                "MqttBridgeConnector": [{}],
                "DataLakeConnector": [{}]
            }
        )
    elif ops_service == "dataprocessor":
        patched.return_value = (
            {},
            {
                "Dataset": [{}],
                "Instance": [{}],
                "Pipeline": [{}]
            }
        )
    elif ops_service == "lnm":
        patched.return_value = (
            {},
            {
                "Lnm": [{}],
                "Scale": [{}]
            }
        )

    yield patched


def assert_dict_props(path: str, expected: str, obj: Dict[str, str]):
    val = obj
    for key in path.split("/"):
        val = val[key]
    if isinstance(val, list) or isinstance(val, dict):
        assert expected in val
    else:
        assert val == expected


def assert_conditions(target: Dict[str, Any], conditions: List[str]):
    target_conditions = target["conditions"]
    for condition in conditions:
        assert condition in target_conditions


def assert_evaluations(target: Dict[str, Any], evaluations: List[List[tuple]]):
    result_evals = target["evaluations"]
    for idx, evals in enumerate(evaluations):
        for eval in evals:
            assert_dict_props(path=eval[0], expected=eval[1], obj=result_evals[idx])


def generate_resource_stub(
    metadata: Dict[str, Any] = {},
    spec: Dict[str, Any] = {},
    status: Dict[str, Any] = {},
):
    resource = {}

    # fill metadata
    resource["metadata"] = {"namespace": "mock_namespace", "name": "mock_name"}
    resource["spec"] = {}
    resource["status"] = {}

    for key in metadata:
        resource["metadata"][key] = metadata[key]
    for key in spec:
        resource["spec"][key] = spec[key]
    for key in status:
        resource["status"][key] = status[key]
    return resource


def assert_check_by_resource_types(ops_service, mocker, mock_resource_types, resource_kinds, eval_lookup):
    # Mock the functions
    for key, value in eval_lookup.items():
        eval_lookup[key] = mocker.patch(value, return_value={})

    # run the checks
    run_checks(
        ops_service=ops_service,
        pre_deployment=False,
        post_deployment=True,
        as_list=False,
        resource_kinds=resource_kinds,
    )

    if not resource_kinds:
        # ensure all checks were run
        [eval_lookup[evaluator].assert_called_once() for evaluator in eval_lookup]
    else:
        # ensure each individual resource kind check was run once
        for resource_kind in resource_kinds:
            eval_lookup[resource_kind].assert_called_once()
            del eval_lookup[resource_kind]
        # ensure no other checks were run
        [eval_lookup[evaluator].assert_not_called() for evaluator in eval_lookup]


@pytest.fixture
def mocked_list_deployments(mocked_client):
    from kubernetes.client.models import V1DeploymentList, V1Deployment, V1ObjectMeta

    def _handle_list_deployments(*args, **kwargs):
        names = ["mock_deployment"]

        deployment_list = []
        for name in names:
            deployment_list.append(V1Deployment(metadata=V1ObjectMeta(namespace="mock_namespace", name=name)))
        deployment_list = V1DeploymentList(items=deployment_list)

        return deployment_list

    mocked_client.AppsV1Api().list_deployment_for_all_namespaces.side_effect = _handle_list_deployments

    yield mocked_client