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

import six


class StrMixin(object):
    """Mixin that helps deal with the PY2 and PY3 method differences.

    http://lucumr.pocoo.org/2011/1/22/forwards-compatible-python/ explains
    why this is quite useful...
    """

    if six.PY2:
        def __str__(self):
            try:
                return self.__bytes__()
            except AttributeError:
                return self.__unicode__().encode('utf-8')
    else:
        def __str__(self):
            return self.__unicode__()
