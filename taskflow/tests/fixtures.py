# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import warnings

import fixtures
from sqlalchemy import exc as sqla_exc


class WarningsFixture(fixtures.Fixture):
    """Filters out warnings during test runs."""
    def setUp(self):
        super().setUp()

        self._original_warning_filters = warnings.filters[:]

        warnings.simplefilter('once', DeprecationWarning)

        # The UUIDFields emits a warning if the value is not a valid UUID.
        # Let's escalate that to an exception in the test to prevent adding
        # violations.

        warnings.filterwarnings('error', message='.*invalid UUID.*')

        # Enable deprecation warnings for taskflow itself to capture upcoming
        # SQLAlchemy changes
        warnings.filterwarnings(
            'ignore',
            category=sqla_exc.SADeprecationWarning,
        )

        warnings.filterwarnings(
            'error',
            module='taskflow',
            category=sqla_exc.SADeprecationWarning,
        )

        # Enable general SQLAlchemy warnings also to ensure we're not doing
        # silly stuff. It's possible that we'll need to filter things out here
        # with future SQLAlchemy versions, but that's a good thing
        warnings.filterwarnings(
            'error',
            module='taskflow',
            category=sqla_exc.SAWarning,
        )
        self.addCleanup(self._reset_warning_filters)

    def _reset_warning_filters(self):
        warnings.filters[:] = self._original_warning_filters
