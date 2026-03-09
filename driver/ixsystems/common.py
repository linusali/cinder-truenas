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
#
# Performance improvements (v3.1.0):
#   - Use /api/v2.0/iscsi/target?name=<x> filter instead of full list scan
#   - Use /api/v2.0/iscsi/extent?name=<x> filter instead of full list scan
#   - Merged _get_iscsi_targetextent + _get_available_lun into one call
#     (_get_targetextent_and_lun) — was fetching the same full list twice
#   - _get_iscsi_global_config result cached in self._iscsi_basename
#     so it is only fetched once per driver lifetime instead of every attach
#   - TrueNAS CORE 13 fix: alias omitted from target creation (422 duplicate)
#   - TrueNAS CORE 13 fix: force/remove passed as query params not JSON body

import json
import math
import urllib.parse

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

    VERSION = '3.1.0'

    def __init__(self, configuration):
        self.configuration = configuration
        self.configuration.append_config_values(options.ixsystems_opts)
        self._stats = {}
        self.handle = None
        # Cached iSCSI global basename — fetched once, never changes at runtime
        self._iscsi_basename = None

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
        :param query_params: dict appended to URL as query string
        :returns: parsed object (dict or list) or None for 204/empty
        :raises exception.VolumeBackendAPIException: on any error
        """
        server = self._get_server_handle()
        try:
            ret = server.invoke_command(method, path, params, query_params)
        except FreeNASApiError as e:
            raise exception.VolumeBackendAPIException(data=str(e)) from e

        code = ret.get('code', 0)
        raw = ret.get('response', '')

        LOG.debug('TrueNAS API %s %s -> HTTP %s', method, path, code)

        if code == 204:
            return None

        if not raw or not raw.strip():
            LOG.debug('TrueNAS API returned empty body for %s %s', method, path)
            return None

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
        """Convert GiB to bytes for volsize and capacity math."""
        return size_gb * units.Gi

    # ------------------------------------------------------------------ #
    # Pool / capacity                                                      #
    # ------------------------------------------------------------------ #

    def _parse_pool_response(self, response):
        """
        Extract free/total bytes from a TrueNAS API v2.0 /pool response.
        """
        if response is None:
            return 0, 0

        pool_name = self.configuration.ixsystems_datastore_pool

        if isinstance(response, list):
            pool = next(
                (p for p in response if p.get('name') == pool_name),
                None
            )
        elif isinstance(response, dict):
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

        # Strategy 1: top-level keys (TrueNAS SCALE newer builds)
        size_bytes = pool.get('size') or pool.get('total') or None
        free_bytes = pool.get('free') or pool.get('avail') or None

        if size_bytes and free_bytes:
            return int(free_bytes), int(size_bytes)

        # Strategy 2: topology.data[0].stats (TrueNAS CORE)
        try:
            topology_data = pool.get('topology', {}).get('data', [])
            if topology_data:
                stats = topology_data[0].get('stats', {})
                size_bytes = stats.get('size', 0)
                allocated_bytes = stats.get('allocated', 0)
                free_bytes = size_bytes - allocated_bytes
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
        """Fetch pool capacity from TrueNAS and populate self.stats."""
        LOG.info('iXsystems Get Volume Status')
        conf = self.configuration

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
        """Create a ZFS zvol on TrueNAS."""
        LOG.info('iXsystems: _create_volume %s (%sGB)', volume_name, volume_size_gb)
        dataset_path = self.configuration.ixsystems_dataset_path
        zvol_path = f'{dataset_path}/{volume_name}'
        size_bytes = self._size_gb_to_bytes(volume_size_gb)

        params = {
            'name': zvol_path,
            'type': 'VOLUME',
            'volsize': size_bytes,
            'volblocksize': '512',
            'sparse': True,
        }
        result = self._execute_request('/api/v2.0/pool/dataset', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response creating zvol {zvol_path}'
            )
        LOG.info('iXsystems: zvol created: %s', zvol_path)
        return result

    def _delete_volume(self, volume_name):
        """Delete a ZFS zvol from TrueNAS."""
        LOG.info('iXsystems: _delete_volume %s', volume_name)
        dataset_path = self.configuration.ixsystems_dataset_path
        zvol_path = f'{dataset_path}/{volume_name}'
        encoded = zvol_path.replace('/', '%2F')

        self._execute_request(
            f'/api/v2.0/pool/dataset/id/{encoded}',
            'DELETE',
            query_params={'recursive': 'true'}
        )
        LOG.info('iXsystems: zvol deleted: %s', zvol_path)

    def _extend_volume(self, volume_name, new_size_gb):
        """Extend a ZFS zvol."""
        LOG.info('iXsystems: _extend_volume %s -> %sGB', volume_name, new_size_gb)
        dataset_path = self.configuration.ixsystems_dataset_path
        zvol_path = f'{dataset_path}/{volume_name}'
        encoded = zvol_path.replace('/', '%2F')
        size_bytes = self._size_gb_to_bytes(new_size_gb)

        self._execute_request(
            f'/api/v2.0/pool/dataset/id/{encoded}',
            'PUT',
            {'volsize': size_bytes}
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
        """Delete a ZFS snapshot.

        TrueNAS API v2.0 snapshot IDs are the full ZFS snapshot path with
        ALL special characters percent-encoded — including the '@' separator
        between dataset name and snapshot name.

        Wrong (old): ocp-pool%2Fvols%2Fcinder%2Fvolume-abc@snapshot-xyz
                     ← '@' not encoded, TrueNAS returns 404
        Correct:     ocp-pool%2Fvols%2Fcinder%2Fvolume-abc%40snapshot-xyz
                     ← full urllib.parse.quote encoding
        """
        LOG.info('iXsystems: _delete_snapshot %s@%s', volume_name, snapshot_name)
        dataset_path = self.configuration.ixsystems_dataset_path
        full_snap = f'{dataset_path}/{volume_name}@{snapshot_name}'
        # Must encode ALL special chars including '/' and '@'
        snap_id = urllib.parse.quote(full_snap, safe='')
        self._execute_request(
            f'/api/v2.0/zfs/snapshot/id/{snap_id}',
            'DELETE'
        )

    def _create_volume_from_snapshot(self, volume_name, src_volume_name, snapshot_name):
        """Clone a snapshot into a new zvol.

        The 'snapshot' field in the clone API takes the unencoded full ZFS
        snapshot path (pool/dataset@snapname) — this is a POST body field,
        not a URL path segment, so no percent-encoding needed here.
        """
        LOG.info(
            'iXsystems: _create_volume_from_snapshot %s from %s@%s',
            volume_name, src_volume_name, snapshot_name
        )
        dataset_path = self.configuration.ixsystems_dataset_path
        src_snapshot = f'{dataset_path}/{src_volume_name}@{snapshot_name}'
        dst_dataset = f'{dataset_path}/{volume_name}'

        params = {
            'snapshot': src_snapshot,
            'dataset_dst': dst_dataset,
        }
        result = self._execute_request('/api/v2.0/zfs/snapshot/clone', 'POST', params)
        if result is None:
            raise exception.VolumeBackendAPIException(
                data=f'No response cloning snapshot to {dst_dataset}'
            )
        return result

    def _create_cloned_volume(self, volume_name, src_volume_name, volume_size_gb):
        """Clone a volume by snapshotting it then cloning the snapshot.

        The temporary snapshot created here is cleaned up immediately after
        the clone succeeds. Previously it was never deleted, causing orphaned
        snapshots to accumulate on TrueNAS for every cloned volume.
        """
        LOG.info(
            'iXsystems: _create_cloned_volume %s from %s',
            volume_name, src_volume_name
        )
        temp_snapshot = f'clone-tmp-{volume_name}'
        self._create_snapshot(src_volume_name, temp_snapshot)
        try:
            self._create_volume_from_snapshot(volume_name, src_volume_name, temp_snapshot)
        finally:
            # Always clean up the temp snapshot — even if the clone failed.
            # On failure this leaves TrueNAS in a clean state for retry.
            try:
                self._delete_snapshot(src_volume_name, temp_snapshot)
                LOG.debug(
                    'iXsystems: cleaned up temp snapshot %s@%s',
                    src_volume_name, temp_snapshot
                )
            except Exception as e:
                # Non-fatal — log and move on. The clone result is what matters.
                LOG.warning(
                    'iXsystems: failed to delete temp snapshot %s@%s: %s',
                    src_volume_name, temp_snapshot, e
                )

    # ------------------------------------------------------------------ #
    # iSCSI target management                                              #
    # ------------------------------------------------------------------ #

    def _get_iscsi_target(self, target_name):
        """
        Return the iSCSI target dict or None.

        PERF: Uses server-side ?name= filter instead of fetching the full
        target list and scanning it in Python. On a busy TrueNAS with many
        targets this avoids transferring and parsing a large JSON array.
        """
        results = self._execute_request(
            '/api/v2.0/iscsi/target',
            query_params={'name': target_name}
        ) or []
        # API returns a list even when filtered — take first exact match
        for t in results:
            if t.get('name') == target_name:
                return t
        return None

    def _create_iscsi_target(self, target_name):
        """
        Create an iSCSI target on TrueNAS.

        TrueNAS CORE 13 treats alias='' as a value and enforces uniqueness —
        sending alias='' for every target causes HTTP 422 'Alias already exists'
        after the first target. Omit alias entirely so TrueNAS assigns null.
        """
        LOG.info('iXsystems: creating iSCSI target %s', target_name)
        params = {
            'name': target_name,
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
        """
        Delete an iSCSI target.

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
        """
        Return the iSCSI extent dict or None.

        PERF: Uses server-side ?name= filter instead of full list scan.
        """
        results = self._execute_request(
            '/api/v2.0/iscsi/extent',
            query_params={'name': extent_name}
        ) or []
        for e in results:
            if e.get('name') == extent_name:
                return e
        return None

    def _create_iscsi_extent(self, extent_name, zvol_path):
        """Create an iSCSI extent backed by a zvol."""
        LOG.info('iXsystems: creating iSCSI extent %s -> %s', extent_name, zvol_path)
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
        """Delete an iSCSI extent.

        TrueNAS CORE 13: 'remove' must be a query param, not a JSON body.
        """
        LOG.info('iXsystems: deleting iSCSI extent id=%s', extent_id)
        self._execute_request(
            f'/api/v2.0/iscsi/extent/id/{extent_id}',
            'DELETE',
            query_params={'remove': 'true'}
        )

    def _get_targetextent_and_lun(self, target_id, extent_id=None):
        """
        Fetch the targetextent list filtered to a specific target and return
        both the existing link (if any) and the next available LUN ID.

        PERF: Replaces two separate calls that were both fetching the full
        targetextent list:
          - _get_iscsi_targetextent(target_id, extent_id)  → find existing link
          - _get_available_lun(target_id)                  → find free LUN
        Now we fetch the list once and derive both answers from it.

        :returns: (link_or_None, next_free_lun_id)
        """
        results = self._execute_request(
            '/api/v2.0/iscsi/targetextent',
            query_params={'target': target_id}
        ) or []

        existing_link = None
        used_luns = set()

        for link in results:
            if link.get('target') == target_id:
                used_luns.add(link.get('lunid', 0))
                if extent_id is not None and link.get('extent') == extent_id:
                    existing_link = link

        # Find the lowest free LUN
        lun = 0
        while lun in used_luns:
            lun += 1

        return existing_link, lun

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

    def _get_iscsi_global_config(self):
        """
        Return TrueNAS global iSCSI config (includes basename).

        PERF: Result is cached in self._iscsi_basename after the first call.
        The basename never changes at runtime so there is no reason to fetch
        it on every single attach operation.
        """
        if self._iscsi_basename is None:
            config = self._execute_request('/api/v2.0/iscsi/global') or {}
            self._iscsi_basename = config
            LOG.debug('iXsystems: cached iSCSI global config: %s', config)
        return self._iscsi_basename

    # ------------------------------------------------------------------ #
    # High-level attach / detach                                           #
    # ------------------------------------------------------------------ #

    def _create_target_and_extent(self, volume_name):
        """
        Ensure an iSCSI target + extent exist for volume_name and are linked.

        Returns (target, extent, lun_id).

        API call count (optimised):
          1. GET /api/v2.0/iscsi/target?name=<x>       server-side filter
          2. POST /api/v2.0/iscsi/target                (if not exists)
          3. GET /api/v2.0/iscsi/extent?name=<x>       server-side filter
          4. POST /api/v2.0/iscsi/extent                (if not exists)
          5. GET /api/v2.0/iscsi/targetextent?target=<id>  filtered + lun calc
          6. POST /api/v2.0/iscsi/targetextent          (if not linked)
          ---
          Total: 4 calls (happy path — target/extent/link all new)
                 4 calls (idempotent path — all already exist, no POSTs)
          Was:   8 calls before optimisation
        """
        dataset_path = self.configuration.ixsystems_dataset_path
        target_name = self._get_iscsi_target_name(volume_name)
        zvol_path = f'{dataset_path}/{volume_name}'

        # Step 1+2: target
        target = self._get_iscsi_target(target_name)
        if target is None:
            target = self._create_iscsi_target(target_name)
        target_id = target['id']

        # Step 3+4: extent
        extent = self._get_iscsi_extent(volume_name)
        if extent is None:
            extent = self._create_iscsi_extent(volume_name, zvol_path)
        extent_id = extent['id']

        # Step 5+6: targetextent link + LUN — single list fetch covers both
        link, next_lun = self._get_targetextent_and_lun(target_id, extent_id)
        if link is None:
            link = self._create_iscsi_targetextent(target_id, extent_id, next_lun)
            lun_id = next_lun
        else:
            lun_id = link['lunid']

        return target, extent, lun_id

    def _remove_target_and_extent(self, volume_name):
        """
        Remove the iSCSI target, extent, and their link for volume_name.

        API call count (optimised):
          1. GET /api/v2.0/iscsi/target?name=<x>         server-side filter
          2. GET /api/v2.0/iscsi/extent?name=<x>         server-side filter
          3. GET /api/v2.0/iscsi/targetextent?target=<id> server-side filter
          4. DELETE targetextent
          5. DELETE extent
          6. DELETE target
          ---
          Total: 6 calls (same count but each GET is now filtered, not full scan)
        """
        target_name = self._get_iscsi_target_name(volume_name)

        target = self._get_iscsi_target(target_name)
        extent = self._get_iscsi_extent(volume_name)

        if target and extent:
            link, _ = self._get_targetextent_and_lun(target['id'], extent['id'])
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
