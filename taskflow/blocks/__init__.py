# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Blocks define *structure*

There are two categories of blocks:
- patterns, which provide convenient way to express basic flow
  structure, like linear flow or parallel flow
- terminals, which run task or needed for housekeeping

"""

# Import most used blocks into this module namespace:
from taskflow.blocks.patterns import LinearFlow    # noqa
from taskflow.blocks.patterns import ParallelFlow  # noqa
from taskflow.blocks.task import Task              # noqa
