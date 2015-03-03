#!/usr/bin/env python
# Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from __future__ import print_function

"""
Perform GET, PUT, POST, and DELETE operations on Rexster.

Required arguments, in they order they are required, include the REST method,
the URL's path, and a base64-encoded EzSecurityToken. Optionally, a base URL can
 be passed as an argument.
"""

import json
import sys

import requests


DEFAULT_BASE_URL = 'http://localhost:8182'
INVALID_INPUT = 1
REQUEST_FAILURE = 2


def request_graph(command, path, token, base_url):
    """
    Perform GET, PUT, POST, and DELETE operations on Rexster.

    Use the given path component of the URL and auths. Print the JSON result.

    Args:
        command (str): Op to perform; get, put, post or delete
        path (str): Path component of URL; the resource to request
        token (str): Base64-encoded EzSecurityToken
        base_url (str): Protocol, host, and port to use
    """

    try:
        if command in ('get', 'post', 'delete', 'put'):
            request = requests.request(command, base_url + path,
                                       auth=(token, 'none'))

        else:
            print("ERROR: Command must be 'get', 'put', 'post', or 'delete'!",
                  file=sys.stderr)

            sys.exit(INVALID_INPUT)

        print(json.dumps(request.json(), indent=4))
    except IOError as e:
        print('ERROR: Unable to make request on Rexster!', file=sys.stderr)
        sys.exit(REQUEST_FAILURE)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['get', 'post', 'put', 'delete'],
                        help="Any lowercase REST method.");

    parser.add_argument('path', help="Path to resource, for example '/graphs'.")
    parser.add_argument('token', help='Base64-encoded security token.')
    parser.add_argument('-b', '--base-url', default=DEFAULT_BASE_URL,
                        help='Protocol, host, and port to use for Rexster.')

    args = parser.parse_args()

    request_graph(args.command, args.path, args.token, args.base_url)