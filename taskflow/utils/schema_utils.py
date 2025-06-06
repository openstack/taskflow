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

import jsonschema
from jsonschema import exceptions as schema_exc


# Expose these types so that people don't have to import the same exceptions.
ValidationError = schema_exc.ValidationError
SchemaError = schema_exc.SchemaError


def schema_validate(data, schema):
    """Validates given data using provided json schema."""
    Validator = jsonschema.validators.validator_for(schema)
    # Special jsonschema validation types/adjustments.
    # See: https://github.com/Julian/jsonschema/issues/148
    type_checker = Validator.TYPE_CHECKER.redefine(
        "array", lambda checker, data: isinstance(data, (list, tuple)))
    TupleAllowingValidator = jsonschema.validators.extend(
        Validator, type_checker=type_checker)
    TupleAllowingValidator(schema).validate(data)
