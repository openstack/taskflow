# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

from debtcollector import removals

from taskflow.persistence import models


# TODO(harlowja): remove me in a future version, since the models
# module is more appropriately named to what the objects in it are used for...
removals.removed_module(__name__,  replacement="'%s'" % models.__name__,
                        version="1.15", removal_version='2.0',
                        stacklevel=4)


# Keep alias classes/functions... around until this module is removed.
LogBook = models.LogBook
FlowDetail = models.FlowDetail
AtomDetail = models.AtomDetail
TaskDetail = models.TaskDetail
RetryDetail = models.RetryDetail
atom_detail_type = models.atom_detail_type
atom_detail_class = models.atom_detail_class
ATOM_TYPES = models.ATOM_TYPES
