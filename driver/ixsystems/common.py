# Copyright (c) 2016, iXsystems Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# Python 3.9 migration notes vs original:
#   - urllib2       → urllib.request / urllib.error
#   - httplib       → http.client
#   - simplejson    → stdlib json (with robust empty-response guard)
#   - has_key(k)    → k in d
#   - except X, e  → except X as e
#   - print stmt   → LOG calls
#   - super()      → super() (no args)
#   - encode/decode bytes explicitly for HTTP body
#   - API v1 storage/volume endpoint → API v2 /pool  (TrueNAS 12+)
#   - Response is now a JSON array from /pool; handled via _parse_pool_response
#   - Empty / non-JSON API responses guarded in _update_volume_stats and elsewhere
#   - Integer division: / → // where whole GiB values needed
#   - String formatting: % → f-strings throughout

import json
import math

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from cinder import exception
from cinder.volume.drivers.ixsystems import options
from cinder.volume.drivers.ixsystems.freenasapi import FreeNASApiError
from cinder.volume.drivers.ixsystems.freenasapi import FreeNASServer

LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class FreeNASCommon:
    """
    Shared logic layer between iscsi.py and the FreeNAS/TrueNAS REST API.

    All public methods that talk to TrueNAS go through _execute_request(),
    which provides consistent error handling and JSON parsing.
    """

    FREENAS_VOLUME_CONTAINER = 'cinder-tank'
    FREENAS_TARGET_GROUP_PORTAL = 1
    FREENAS_TARGET_GROUP_INITIATOR = 1
    FREENAS_TARGET_GROUP_AUTH = 'None'
    FREENAS_TARGET_GROUP_AUTH_TYPE = 'None'
    FREENAS_TARGET_GROUP_INITIALDIGEST = 'Auto'

    VERSION = '3.0.0'

    def __init__(self, configuration):
        self.configuration = configuration
        self.configuration.append_config_values(options.ixsystems_opts)
        self._stats = {}
        self.handle = None

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _get_server_handle(self):
        """Return (and lazily create) a FreeNASServer connection handle."""
        if self.handle is None:
            conf = self.configuration
            self.handle = FreeNASServer(
                host=conf.ixsystems_server_hostname,
                username=conf.ixsystems_login,
                password=conf.ixsystems_password,
                apikey=conf.ixsystems_apikey,
                transport_type=conf.ixsystems_transport_type,
            )
        return self.handle

    def _execute_request(self, path, method='GET', params=None, query_params=None):
        """
        Execute a REST call and return the parsed JSON body.

        :param path: API path, e.g. '/api/v2.0/pool'
        :param method: GET | POST | PUT | DELETE
        :param params: dict payload (will be JSON-encoded for POST/PUT)
        :returns: parsed object (dict or list) or None for 204/empty
        :raises exception.VolumeBackendAPIException: on any error
        """
        server = self._get_server_handle()
        try:
            ret = server.invoke_command(method, path, params, query_params)
        except FreeNASApiError as e:
            raise exception.VolumeBackendAPIException(data=str(e)) from e

        # ret is expected to be {'code': int, 'response': str}
        code = ret.get('code', 0)
        raw = ret.get('response', '')

        LOG.debug('TrueNAS API %s %s → HTTP %s', method, path, code)

        # HTTP 204 No Content — success with no body
        if code == 204:
            return None

        # Guard: empty or whitespace-only body
        if not raw or not raw.strip():
            LOG.debug('TrueNAS API returned empty body for %s %s', method, path)
            return None

        # Guard: bare JSON string e.g. '""' or '"some error"'
        # These are not the dicts/lists we want — treat as empty
        stripped = raw.strip()
        if stripped.startswith('"') and stripped.endswith('"'):
            LOG.warning(
                'TrueNAS API returned bare JSON string for %s %s: %s',
                method, path, stripped
            )
            return None

        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            LOG.error(
                'TrueNAS API JSON parse error for %s %s: %s | body: %.200s',
                method, path, e, raw
            )
            raise exception.VolumeBackendAPIException(
                data=f'Invalid JSON from TrueNAS API {method} {path}: {e}'
            ) from e

    def _get_iscsi_target_name(self, volume_name):
        """Build the iSCSI target name for a given volume."""
        iqn_prefix = self.configuration.ixsystems_iqn_prefix.rstrip(':')
        return f'{iqn_prefix}:{volume_name}'

    def _size_bytes_to_gb(self, size_bytes):
        """Convert byte value to GiB, rounding up."""
        return math.ceil(size_bytes / units.Gi)

    def _size_gb_to_bytes(self, size_gb):
        """Convert GiB to bytes."""
        return size_gb * units.Gi

    # ------------------------------------------------------------------ #
    # Pool / capacity                                                      #
    # ------------------------------------------------------------------ #

    def _parse_pool_response(self, response):
        """
        Extract free/total bytes from a TrueNAS API v2.0 /pool response.

        TrueNAS v2.0 returns a LIST of pool objects.
        TrueNAS v1 returned a single dict — we handle both.
        """
        if response is None:
            return 0, 0

        pool_name = self.configuration.ixsystems_datastore_pool

        # v2.0 returns a list
        if isinstance(response, list):
            pool = next(
                (p for p in response if p.get('name') == pool_name),
                None
            )
        elif isinstance(response, dict):
            # v1 or single-pool response
            pool = response
        else:
            LOG.warning('Unexpected pool response type: %s', type(response))
            return 0, 0

        if pool is None:
            LOG.warning(
                'Pool "%s" not found in TrueNAS response. Available: %s',
                pool_name,
                [p.get('name') for p in response]
                if isinstance(response, list) else 'N/A'
            )
            return 0, 0

        LOG.debug('TrueNAS raw pool object: %s', pool)

        # ----------------------------------------------------------------
        # Capacity extraction — TrueNAS CORE (this API version) does NOT
        # expose free/size at the top level of the pool object.
        #
        # Actual structure (confirmed from API response):
        #   pool['topology']['data'][0]['stats']['size']       ← total bytes
        #   pool['topology']['data'][0]['stats']['allocated']  ← used bytes
        #   free = size - allocated
        #
        # TrueNAS SCALE newer versions may expose top-level 'free'/'size'
        # so we try that first and fall back to topology.
        # ----------------------------------------------------------------

        # Strategy 1: top-level keys (TrueNAS SCALE newer builds)
        size_bytes = pool.get('size') or pool.get('total') or None
        free_bytes = pool.get('free') or pool.get('avail') or None

        if size_bytes and free_bytes:
            LOG.debug(
                'iXsystems: capacity from top-level keys: '
                'size=%s free=%s', size_bytes, free_bytes
            )
            return int(free_bytes), int(size_bytes)

        # Strategy 2: topology.data[0].stats (TrueNAS CORE / this version)
        try:
            topology_data = pool.get('topology', {}).get('data', [])
            if topology_data:
                stats = topology_data[0].get('stats', {})
                size_bytes = stats.get('size', 0)
                allocated_bytes = stats.get('allocated', 0)
                free_bytes = size_bytes - allocated_bytes
                LOG.debug(
                    'iXsystems: capacity from topology.data[0].stats: '
                    'size=%s allocated=%s free=%s',
                    size_bytes, allocated_bytes, free_bytes
                )
                if size_bytes > 0:
                    return int(free_bytes), int(size_bytes)
        except (IndexError, KeyError, TypeError) as e:
            LOG.warning('iXsystems: failed to parse topology stats: %s', e)

        # Strategy 3: top-level allocated/used fallback
        allocated = pool.get('allocated') or pool.get('used') or 0
        if size_bytes and allocated:
            free_bytes = size_bytes - allocated
            return int(free_bytes), int(size_bytes)

        LOG.warning(
            'iXsystems: could not extract capacity from pool "%s". '
            'Pool keys: %s', pool_name, list(pool.keys())
        )
        return 0, 0

    def _update_volume_stats(self):
        """
        Fetch pool capacity from TrueNAS and populate self.stats.

        Called by iscsi.py get_volume_stats().

        Python 3.9 fixes vs original:
          - Use /api/v2.0/pool instead of /api/v1.0/storage/volume/
          - Guard empty/null response before json.loads()
          - Handle list response (v2.0) not just dict (v1)
          - Integer division with // for GiB calculation
        """
        LOG.info('iXsystems Get Volume Status')
        conf = self.configuration

        # TrueNAS API v2.0 endpoint
        response = self._execute_request('/api/v2.0/pool')

        free_bytes, total_bytes = self._parse_pool_response(response)

        free_gb = free_bytes / units.Gi
        total_gb = total_bytes / units.Gi

        self.stats = {
            'volume_backend_name': conf.ixsystems_volume_backend_name,
            'vendor_name': conf.ixsystems_vendor_name,
            'driver_version': self.VERSION,
            'storage_protocol': conf.ixsystems_storage_protocol,
            'total_capacity_gb': round(total_gb, 2),
            'free_capacity_gb': round(free_gb, 2),
            'reserved_percentage': 0,
            'QoS_support': False,
            'multiattach': True,
        }

        LOG.info(
            'iXsystems capacity: total=%.2fGB free=%.2fGB',
            total_gb, free_gb
        )
        return self.stats

    # ------------------------------------------------------------------ #
    # Volume operations                                                    #
    # ------------------------------------------------------------------ #

    def _create_volume(self, volume_name, volume_size_gb):
        """
        Create a ZFS zvol on TrueNAS.

        :param volume_name: name of the zvol (no path prefix)
        :param volume_size_gb: size in GiB
        """
        LOG.info('iXsystems: _create_volume %s (%sGB)', volume_name, volume_size_gb)
        dataset_path = self.configuration.ixsystems_dataset_path
        full_name = f'{dataset_path}/{volume_name}'

        params = {
            'name': full_name,
            'type': 'VOLUME',
            'volsize': self._size_gb_to_bytes(volume_size_gb),
            'volblocksize': '512',
            'sparse': False,
        }
        result = self._execute_request('/api/v2.0/pool/dataset', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response creating zvol {full_name}'
            )
        LOG.info('iXsystems: zvol created: %s', full_name)
        return result

    def _delete_volume(self, volume_name):
        """Delete a ZFS zvol from TrueNAS."""
        LOG.info('iXsystems: _delete_volume %s', volume_name)
        dataset_path = self.configuration.ixsystems_dataset_path
        zvol_id = f'{dataset_path}/{volume_name}'.replace('/', '%2F')

        # Check it exists first; if not, skip silently
        existing = self._execute_request(f'/api/v2.0/pool/dataset/id/{zvol_id}')
        if existing is None:
            LOG.warning('iXsystems: zvol %s not found, skipping delete', volume_name)
            return

        self._execute_request(
            f'/api/v2.0/pool/dataset/id/{zvol_id}',
            'DELETE',
            {'recursive': True}
        )
        LOG.info('iXsystems: zvol deleted: %s/%s', dataset_path, volume_name)

    def _extend_volume(self, volume_name, new_size_gb):
        """Extend a ZFS zvol to new_size_gb."""
        LOG.info('iXsystems: _extend_volume %s → %sGB', volume_name, new_size_gb)
        dataset_path = self.configuration.ixsystems_dataset_path
        zvol_id = f'{dataset_path}/{volume_name}'.replace('/', '%2F')

        params = {'volsize': self._size_gb_to_bytes(new_size_gb)}
        self._execute_request(
            f'/api/v2.0/pool/dataset/id/{zvol_id}',
            'PUT',
            params
        )

    def _create_snapshot(self, volume_name, snapshot_name):
        """Create a ZFS snapshot."""
        LOG.info('iXsystems: _create_snapshot %s@%s', volume_name, snapshot_name)
        dataset_path = self.configuration.ixsystems_dataset_path
        params = {
            'dataset': f'{dataset_path}/{volume_name}',
            'name': snapshot_name,
        }
        result = self._execute_request('/api/v2.0/zfs/snapshot', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response creating snapshot {volume_name}@{snapshot_name}'
            )
        return result

    def _delete_snapshot(self, volume_name, snapshot_name):
        """Delete a ZFS snapshot."""
        LOG.info('iXsystems: _delete_snapshot %s@%s', volume_name, snapshot_name)
        dataset_path = self.configuration.ixsystems_dataset_path
        snap_id = f'{dataset_path}/{volume_name}@{snapshot_name}'.replace('/', '%2F')
        self._execute_request(
            f'/api/v2.0/zfs/snapshot/id/{snap_id}',
            'DELETE'
        )

    def _create_volume_from_snapshot(self, volume_name, snapshot_volume_name,
                                     snapshot_name):
        """Clone a snapshot into a new zvol."""
        LOG.info(
            'iXsystems: _create_volume_from_snapshot %s from %s@%s',
            volume_name, snapshot_volume_name, snapshot_name
        )
        dataset_path = self.configuration.ixsystems_dataset_path
        src_snapshot = f'{dataset_path}/{snapshot_volume_name}@{snapshot_name}'
        dst_dataset = f'{dataset_path}/{volume_name}'

        params = {
            'snapshot': src_snapshot,
            'dataset_dst': dst_dataset,
        }
        result = self._execute_request(
            '/api/v2.0/zfs/snapshot/clone', 'POST', params
        )
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response cloning snapshot to {dst_dataset}'
            )
        return result

    def _create_cloned_volume(self, volume_name, src_volume_name, volume_size_gb):
        """Clone a volume by snapshotting it then cloning the snapshot."""
        LOG.info(
            'iXsystems: _create_cloned_volume %s from %s',
            volume_name, src_volume_name
        )
        temp_snapshot = f'clone-tmp-{volume_name}'
        self._create_snapshot(src_volume_name, temp_snapshot)
        self._create_volume_from_snapshot(volume_name, src_volume_name, temp_snapshot)

    # ------------------------------------------------------------------ #
    # iSCSI target management                                              #
    # ------------------------------------------------------------------ #

    def _get_iscsi_target(self, target_name):
        """Return the iSCSI target dict or None."""
        targets = self._execute_request('/api/v2.0/iscsi/target') or []
        for t in targets:
            if t.get('name') == target_name:
                return t
        return None

    def _create_iscsi_target(self, target_name):
        """Create an iSCSI target on TrueNAS."""
        LOG.info('iXsystems: creating iSCSI target %s', target_name)
        params = {
            'name': target_name,
            'alias': '',
            'mode': 'ISCSI',
            'groups': [],
        }
        result = self._execute_request('/api/v2.0/iscsi/target', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response creating iSCSI target {target_name}'
            )
        return result

    def _delete_iscsi_target(self, target_id):
        """Delete an iSCSI target.

        TrueNAS CORE 13 requires 'force' as a URL query parameter, NOT a
        JSON body -- sending it as JSON body returns HTTP 422 'Not a boolean'.
        """
        LOG.info('iXsystems: deleting iSCSI target id=%s', target_id)
        self._execute_request(
            f'/api/v2.0/iscsi/target/id/{target_id}',
            'DELETE',
            query_params={'force': 'true'}
        )

    def _get_iscsi_extent(self, extent_name):
        """Return the iSCSI extent dict or None."""
        extents = self._execute_request('/api/v2.0/iscsi/extent') or []
        for e in extents:
            if e.get('name') == extent_name:
                return e
        return None

    def _create_iscsi_extent(self, extent_name, zvol_path):
        """Create an iSCSI extent backed by a zvol."""
        LOG.info('iXsystems: creating iSCSI extent %s → %s', extent_name, zvol_path)
        params = {
            'name': extent_name,
            'type': 'DISK',
            'disk': f'zvol/{zvol_path}',
            'blocksize': 512,
            'insecure_tpc': True,
            'xen': False,
            'rpm': 'SSD',
            'ro': False,
        }
        result = self._execute_request('/api/v2.0/iscsi/extent', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response creating iSCSI extent {extent_name}'
            )
        return result

    def _delete_iscsi_extent(self, extent_id):
        """Delete an iSCSI extent."""
        LOG.info('iXsystems: deleting iSCSI extent id=%s', extent_id)
        # TrueNAS CORE 13: 'remove' must be a query param, not a JSON body
        self._execute_request(
            f'/api/v2.0/iscsi/extent/id/{extent_id}',
            'DELETE',
            query_params={'remove': 'true'}
        )

    def _get_iscsi_targetextent(self, target_id, extent_id):
        """Return the target-extent link or None."""
        links = self._execute_request('/api/v2.0/iscsi/targetextent') or []
        for link in links:
            if link.get('target') == target_id and link.get('extent') == extent_id:
                return link
        return None

    def _create_iscsi_targetextent(self, target_id, extent_id, lun_id=0):
        """Associate an extent to a target at the given LUN ID."""
        LOG.info(
            'iXsystems: mapping extent %s to target %s at LUN %s',
            extent_id, target_id, lun_id
        )
        params = {
            'target': target_id,
            'lunid': lun_id,
            'extent': extent_id,
        }
        result = self._execute_request(
            '/api/v2.0/iscsi/targetextent', 'POST', params
        )
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response mapping extent {extent_id} to target {target_id}'
            )
        return result

    def _delete_iscsi_targetextent(self, targetextent_id):
        """Remove a target-extent association."""
        LOG.info('iXsystems: removing targetextent id=%s', targetextent_id)
        self._execute_request(
            f'/api/v2.0/iscsi/targetextent/id/{targetextent_id}',
            'DELETE'
        )

    def _get_available_lun(self, target_id):
        """Return the lowest unused LUN ID for a target."""
        links = self._execute_request('/api/v2.0/iscsi/targetextent') or []
        used = {
            link['lunid']
            for link in links
            if link.get('target') == target_id
        }
        lun = 0
        while lun in used:
            lun += 1
        return lun

    def _get_iscsi_global_config(self):
        """Return TrueNAS global iSCSI config (includes basename)."""
        return self._execute_request('/api/v2.0/iscsi/global') or {}

    # ------------------------------------------------------------------ #
    # High-level attach / detach                                           #
    # ------------------------------------------------------------------ #

    def _create_target_and_extent(self, volume_name):
        """
        Ensure an iSCSI target + extent exist for volume_name and are linked.
        Returns (target, extent, lun_id).
        """
        dataset_path = self.configuration.ixsystems_dataset_path
        target_name = self._get_iscsi_target_name(volume_name)
        zvol_path = f'{dataset_path}/{volume_name}'

        target = self._get_iscsi_target(target_name)
        if target is None:
            target = self._create_iscsi_target(target_name)
        target_id = target['id']

        extent = self._get_iscsi_extent(volume_name)
        if extent is None:
            extent = self._create_iscsi_extent(volume_name, zvol_path)
        extent_id = extent['id']

        link = self._get_iscsi_targetextent(target_id, extent_id)
        if link is None:
            lun_id = self._get_available_lun(target_id)
            link = self._create_iscsi_targetextent(target_id, extent_id, lun_id)
        else:
            lun_id = link['lunid']

        return target, extent, lun_id

    def _remove_target_and_extent(self, volume_name):
        """Remove the iSCSI target, extent, and their link for volume_name."""
        target_name = self._get_iscsi_target_name(volume_name)

        target = self._get_iscsi_target(target_name)
        extent = self._get_iscsi_extent(volume_name)

        if target and extent:
            link = self._get_iscsi_targetextent(target['id'], extent['id'])
            if link:
                self._delete_iscsi_targetextent(link['id'])
            self._delete_iscsi_extent(extent['id'])
            self._delete_iscsi_target(target['id'])
        elif target:
            self._delete_iscsi_target(target['id'])
        elif extent:
            self._delete_iscsi_extent(extent['id'])
        else:
            LOG.warning(
                'iXsystems: no target or extent found for %s on detach',
                volume_name
            )
