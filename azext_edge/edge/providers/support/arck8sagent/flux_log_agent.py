# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

from functools import partial
from typing import Iterable

from knack.log import get_logger

from ..base import (
    DAY_IN_SECONDS,
    process_deployments,
    process_replicasets,
    process_services,
    process_v1_pods,
)

logger = get_logger(__name__)

FLUX_LOG_AGENT_MONIKER = "fluxlogagent"
FLUX_LOG_AGENT_COMPONENT_LABEL = "app.kubernetes.io/component in (flux-logs-agent)"


def fetch_pods(since_seconds: int = DAY_IN_SECONDS):
    dataprocessor_pods = process_v1_pods(
        moniker=FLUX_LOG_AGENT_MONIKER,
        label_selector=FLUX_LOG_AGENT_COMPONENT_LABEL,
        since_seconds=since_seconds,
    )

    return dataprocessor_pods


def fetch_deployments():
    processed = process_deployments(moniker=FLUX_LOG_AGENT_MONIKER, label_selector=FLUX_LOG_AGENT_COMPONENT_LABEL)

    return processed


def fetch_replicasets():
    processed = process_replicasets(moniker=FLUX_LOG_AGENT_MONIKER, label_selector=FLUX_LOG_AGENT_COMPONENT_LABEL)

    return processed


def fetch_services():
    return  process_services(moniker=FLUX_LOG_AGENT_MONIKER, label_selector="app.kubernetes.io/managed-by in (Helm)", prefix_names=["flux-logs-agent"])


support_runtime_elements = {
    "replicasets": fetch_replicasets,
    "services": fetch_services,
    "deployments": fetch_deployments,
}


def prepare_bundle(log_age_seconds: int = DAY_IN_SECONDS) -> dict:
    agent_to_run = {}

    support_runtime_elements["pods"] = partial(fetch_pods, since_seconds=log_age_seconds)
    agent_to_run.update(support_runtime_elements)

    return agent_to_run
