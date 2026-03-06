# Copyright (c) 2016, iXsystems Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Python 3.9 migration notes vs original:
#   - urllib2       → urllib.request / urllib.error
#   - httplib       → http.client
#   - simplejson    → stdlib json
#   - print stmt   → LOG calls
#   - except X, e  → except X as e
#   - base64 encode/decode explicitly (bytes vs str)
#   - super()      → super() no args
#   - encode/decode bytes explicitly for HTTP body
#
# TrueNAS CORE 13 fix:
#   - DELETE endpoints do NOT accept a JSON body for boolean flags like
#     'force' or 'remove' — they return HTTP 422 "Not a boolean" if you
#     send them as JSON. These must be passed as URL query string params.
#   - invoke_command() now accepts a separate `query_params` dict that is
#     always appended to the URL as a query string, regardless of method.
#   - For DELETE calls, callers should use query_params instead of params.

import base64
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class FreeNASApiError(Exception):
    """Raised when the FreeNAS/TrueNAS API returns an error."""

    def __init__(self, message='Unknown error', reason=None):
        full = f'FREENAS api failed. Reason - {message}: {reason}' if reason else message
        super().__init__(full)
        self.message = full


class FreeNASServer:
    """
    Low-level HTTP(S) client for the TrueNAS REST API.

    Used by FreeNASCommon (common.py) to make raw API calls.
    Returns a dict: {'code': <int HTTP status>, 'response': <str body>}
    """

    TRANSPORT_HTTP = 'http'
    TRANSPORT_HTTPS = 'https'

    DEFAULT_PORT_HTTP = 80
    DEFAULT_PORT_HTTPS = 443

    def __init__(self, host, username=None, password=None,
                 apikey=None, transport_type='http', port=None,
                 style='login_password'):
        self.host = host
        self.username = username
        self.password = password
        self.apikey = apikey
        self.transport_type = transport_type
        self.style = style

        if port:
            self.port = port
        else:
            self.port = (
                self.DEFAULT_PORT_HTTPS
                if transport_type == self.TRANSPORT_HTTPS
                else self.DEFAULT_PORT_HTTP
            )

    @property
    def _base_url(self):
        return f'{self.transport_type}://{self.host}:{self.port}'

    def _build_auth_header(self):
        """Return the Authorization header value."""
        if self.apikey:
            return f'Bearer {self.apikey}'
        if self.username and self.password:
            creds = f'{self.username}:{self.password}'
            encoded = base64.b64encode(creds.encode('utf-8')).decode('ascii')
            return f'Basic {encoded}'
        return None

    def _get_ssl_context(self):
        """Return an unverified SSL context for self-signed certificates."""
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def invoke_command(self, method, path, params=None, query_params=None):
        """
        Make an HTTP request to the TrueNAS API.

        :param method:       'GET', 'POST', 'PUT', 'DELETE'
        :param path:         API path, e.g. '/api/v2.0/pool'
        :param params:       dict payload encoded as JSON body (POST/PUT only)
        :param query_params: dict appended to URL as query string.
                             Use this for DELETE boolean flags on TrueNAS CORE 13
                             (e.g. force=true, remove=true) because CORE 13
                             rejects these as a JSON body with HTTP 422.
        :returns: {'code': <int>, 'response': <str>}
        :raises FreeNASApiError: on HTTP or connection errors
        """
        # Build URL — append query_params as query string if provided
        url = f'{self._base_url}{path}'
        if query_params:
            url = f'{url}?{urllib.parse.urlencode(query_params)}'

        # Build body — only for POST/PUT, never for GET/DELETE
        body = None
        if params is not None and method in ('POST', 'PUT', 'PATCH'):
            body = json.dumps(params).encode('utf-8')

        req = urllib.request.Request(url, data=body, method=method)
        req.add_header('Content-Type', 'application/json')
        req.add_header('Accept', 'application/json')

        auth = self._build_auth_header()
        if auth:
            req.add_header('Authorization', auth)

        LOG.debug('FreeNASServer: %s %s', method, url)

        try:
            if self.transport_type == self.TRANSPORT_HTTPS:
                ctx = self._get_ssl_context()
                resp = urllib.request.urlopen(req, context=ctx)
            else:
                resp = urllib.request.urlopen(req)

            status = resp.status
            body_bytes = resp.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''

            LOG.debug('FreeNASServer: HTTP %s <- %s %s', status, method, path)
            return {'code': status, 'response': body_str}

        except urllib.error.HTTPError as e:
            body_bytes = e.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            LOG.error(
                'FreeNASServer: HTTP %s error for %s %s: %s',
                e.code, method, path, body_str
            )
            raise FreeNASApiError(
                f'HTTP {e.code}',
                f'{method} {path} -> {body_str[:200]}'
            ) from e

        except urllib.error.URLError as e:
            LOG.error(
                'FreeNASServer: connection error %s %s: %s',
                method, path, e.reason
            )
            raise FreeNASApiError(
                'Connection error',
                f'{method} {path} -> {e.reason}'
            ) from e
