# Copyright (c) 2016, iXsystems Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Python 3.9 migration notes:
#   - No structural changes needed; oslo.config is already Python 3 compatible.
#   - Removed legacy `from __future__ import` statements (not needed in Py3).
#   - Cleaned up string formatting.

from oslo_config import cfg

ixsystems_opts = [
    cfg.StrOpt(
        'ixsystems_login',
        default='',
        help='Username for TrueNAS API authentication. '
             'Use ixsystems_apikey instead where possible.',
    ),
    cfg.StrOpt(
        'ixsystems_password',
        default='',
        secret=True,
        help='Password for TrueNAS API authentication. '
             'Use ixsystems_apikey instead where possible.',
    ),
    cfg.StrOpt(
        'ixsystems_apikey',
        default='',
        secret=True,
        help='API key for TrueNAS API v2.0 authentication. '
             'Preferred over username/password.',
    ),
    cfg.StrOpt(
        'ixsystems_server_hostname',
        default='',
        help='Hostname or IP address of the TrueNAS server.',
    ),
    cfg.StrOpt(
        'ixsystems_transport_type',
        default='http',
        choices=['http', 'https'],
        help='Transport protocol for TrueNAS API calls (http or https).',
    ),
    cfg.IntOpt(
        'ixsystems_server_port',
        default=None,
        help='TCP port for TrueNAS API. Defaults to 80 (http) or 443 (https).',
    ),
    cfg.StrOpt(
        'ixsystems_volume_backend_name',
        default='iXsystems_TRUENAS_Storage',
        help='Backend name reported to Cinder scheduler.',
    ),
    cfg.StrOpt(
        'ixsystems_iqn_prefix',
        default='iqn.2005-10.org.freenas.ctl',
        help='iSCSI IQN prefix (Base Name). Find it at: '
             'TrueNAS UI -> Sharing -> Block iSCSI -> '
             'Target Global Configuration -> Base Name.',
    ),
    cfg.StrOpt(
        'ixsystems_datastore_pool',
        default='',
        help='ZFS pool name on TrueNAS where zvols will be created, e.g. "tank".',
    ),
    cfg.StrOpt(
        'ixsystems_dataset_path',
        default='',
        help='Full dataset path including pool for zvol creation. '
             'Example: "tank/cinder" or just "tank".',
    ),
    cfg.StrOpt(
        'ixsystems_vendor_name',
        default='iXsystems',
        help='Vendor name reported by the driver.',
    ),
    cfg.StrOpt(
        'ixsystems_storage_protocol',
        default='iscsi',
        help='Storage protocol. Currently only "iscsi" is supported.',
    ),
    cfg.BoolOpt(
        'ixsystems_verify_ssl',
        default=False,
        help='Whether to verify the TrueNAS SSL certificate. '
             'Set True in production with a valid cert.',
    ),
    cfg.IntOpt(
        'ixsystems_portal_group_id',
        default=1,
        help='TrueNAS iSCSI portal group ID to assign to new targets. '
             'TrueNAS CORE will not open port 3260 until at least one target '
             'has a portal group assigned. Find the ID at: '
             'TrueNAS UI -> Sharing -> Block iSCSI -> Portals. '
             'Default is 1 (the out-of-the-box default portal group).',
    ),
    cfg.IntOpt(
        'ixsystems_initiator_group_id',
        default=None,
        help='TrueNAS iSCSI initiator group ID to assign to new targets. '
             'If set, restricts which initiators can connect. '
             'Leave unset (None) to allow all initiators (open access). '
             'Find the ID at: TrueNAS UI -> Sharing -> Block iSCSI -> Initiators.',
    ),
]
