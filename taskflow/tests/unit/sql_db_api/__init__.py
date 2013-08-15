# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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

import os

from os import path
from taskflow.persistence.backends import api as b_api
from taskflow.persistence.backends.sqlalchemy import models


def setUpModule():
    b_api.configure('db_backend')
    b_api.SQL_CONNECTION = 'sqlite:///test.db'

    if not path.isfile('test.db'):
        models.create_tables()


def tearDownModule():
    os.remove('test.db')
