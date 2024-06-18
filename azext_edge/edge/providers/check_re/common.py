# coding=utf-8
# ----------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License file in the project root for license information.
# ----------------------------------------------------------------------------------------------

"""
shared: Define shared data types(enums) and constant strings for IoT Operations service checks.

"""

from enum import Enum

from ...common import ListableEnum


class ResourceOutputDetailLevel(ListableEnum):
    """
    Level of detail in check output.
    """
    SUMMARY = "summary"
    SIMPLIFIED = "simplified"
    EXPANDED = "expanded"

    
