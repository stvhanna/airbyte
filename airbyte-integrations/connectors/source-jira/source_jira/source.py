#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from typing import Mapping, Any, List, Tuple, Optional

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .api import ApplicationRoles, Avatars, Dashboards, Filters, FilterSharing, Groups, Issues, IssueComments, \
    IssueFields, IssueFieldConfigurations, IssueCustomFieldContexts, IssueLinkTypes, IssueNavigatorSettings, \
    IssueNotificationSchemes, IssuePriorities, IssueProperties, IssueRemoteLinks, IssueResolutions, \
    IssueSecuritySchemes, IssueTypeSchemes, IssueTypeScreenSchemes, IssueVotes, IssueWatchers, IssueWorklogs
from .auth import JiraBasicAuthAuthenticator


class SourceJira(AbstractSource):

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = JiraBasicAuthAuthenticator(config["email"], config["api_token"])
        args = {"authenticator": authenticator, "domain": config["domain"]}
        return [
            # ApplicationRoles(**args),
            # Avatars(**args),
            # Dashboards(**args),
            # Filters(**args),
            # FilterSharing(**args),
            # Issues(**args),
            # IssueComments(**args),
            # IssueFields(**args),
            # IssueFieldConfigurations(**args),
            # IssueCustomFieldContexts(**args),
            # IssueLinkTypes(**args),
            # IssueNavigatorSettings(**args),
            # IssueNotificationSchemes(**args),
            # IssuePriorities(**args),
            # IssueProperties(**args),
            # IssueRemoteLinks(**args),
            # IssueResolutions(**args),
            # IssueSecuritySchemes(**args),
            # IssueTypeSchemes(**args),
            # IssueTypeScreenSchemes(**args),
            # IssueVotes(**args),
            # IssueWatchers(**args),
            IssueWorklogs(**args),
        ]
