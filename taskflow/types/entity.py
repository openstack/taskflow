# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Rackspace Inc. All Rights Reserved.
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


class Entity(object):
    """Entity object that identifies some resource/item/other.

    :ivar kind: **immutable** type/kind that identifies this
                entity (typically unique to a library/application)
    :type kind: string
    :ivar Entity.name: **immutable** name that can be used to uniquely
                identify this entity among many other entities
    :type name: string
    :ivar metadata: **immutable** dictionary of metadata that is
                    associated with this entity (and typically
                    has keys/values that further describe this
                    entity)
    :type metadata: dict
    """
    def __init__(self, kind, name, metadata):
        self.kind = kind
        self.name = name
        self.metadata = metadata

    def to_dict(self):
        return {
            'kind': self.kind,
            'name': self.name,
            'metadata': self.metadata
        }
