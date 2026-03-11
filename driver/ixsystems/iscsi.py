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
#   - super() with no args
#   - except X as e  (was except X, e)
#   - f-strings throughout
#   - Delegates all TrueNAS logic to self.common (FreeNASCommon)
#   - Added create_export / ensure_export / remove_export (no-ops)
#   - Multi-attach: terminate_connection only tears down target on last detach

from oslo_config import cfg
from oslo_log import log as logging

from cinder import interface
from cinder.volume import driver
from cinder.volume.drivers.ixsystems import options
from cinder.volume.drivers.ixsystems.common import FreeNASCommon

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


@interface.volumedriver
class FreeNASISCSIDriver(driver.ISCSIDriver):
    """
    OpenStack Cinder iSCSI driver for TrueNAS/FreeNAS (>= 12.x, API v2.0).

    All storage operations are delegated to FreeNASCommon which communicates
    with TrueNAS via FreeNASServer (freenasapi.py).

    Version history:
        1.0.0 - Initial FreeNAS driver (Python 2.7, API v1)
        2.0.0 - TrueNAS 12.x, Python 3 migration attempt
        3.0.0 - Full Python 3.9 migration; API v2.0; robust JSON handling;
                urllib.request replacing urllib2; stdlib json replacing simplejson;
                added create_export/ensure_export/remove_export no-ops;
                multi-attach support with safe terminate_connection
    """

    VERSION = '3.0.0'
    CI_WIKI_NAME = 'iXsystems_TrueNAS_CI'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.configuration.append_config_values(options.ixsystems_opts)
        self.common = None

    @staticmethod
    def get_driver_options():
        return options.ixsystems_opts

    def _init_common(self):
        """Lazily create the FreeNASCommon helper."""
        if self.common is None:
            self.common = FreeNASCommon(self.configuration)

    # ------------------------------------------------------------------ #
    # Driver lifecycle                                                     #
    # ------------------------------------------------------------------ #

    def do_setup(self, context):
        LOG.info('iXsystems: Init Cinder Driver')
        self._init_common()
        LOG.info('iXsystems Do Setup')

    def check_for_setup_error(self):
        LOG.info('iXSystems: Check For Setup Error')
        self._init_common()
        LOG.info('iXSystems: Check For Setup Error')

    # ------------------------------------------------------------------ #
    # Volume operations                                                    #
    # ------------------------------------------------------------------ #

    def create_volume(self, volume):
        LOG.info('iXsystems: create_volume %s size=%sGB', volume.name, volume.size)
        self.common._create_volume(volume.name, volume.size)

    def delete_volume(self, volume):
        LOG.info('iXsystems: delete_volume %s', volume.name)
        self.common._delete_volume(volume.name)

    def extend_volume(self, volume, new_size):
        LOG.info('iXsystems: extend_volume %s -> %sGB', volume.name, new_size)
        self.common._extend_volume(volume.name, new_size)

    def create_snapshot(self, snapshot):
        LOG.info('iXsystems: create_snapshot %s of %s',
                 snapshot.name, snapshot.volume_name)
        self.common._create_snapshot(snapshot.volume_name, snapshot.name)

    def delete_snapshot(self, snapshot):
        LOG.info('iXsystems: delete_snapshot %s', snapshot.name)
        self.common._delete_snapshot(snapshot.volume_name, snapshot.name)

    def create_volume_from_snapshot(self, volume, snapshot):
        LOG.info('iXsystems: create_volume_from_snapshot %s from %s@%s',
                 volume.name, snapshot.volume_name, snapshot.name)
        self.common._create_volume_from_snapshot(
            volume.name, snapshot.volume_name, snapshot.name
        )

    def create_cloned_volume(self, volume, src_vref):
        LOG.info('iXsystems: create_cloned_volume %s from %s',
                 volume.name, src_vref.name)
        self.common._create_cloned_volume(
            volume.name, src_vref.name, volume.size, src_vref.size
        )

    # ------------------------------------------------------------------ #
    # Export methods — required by Cinder ISCSIDriver base class          #
    #                                                                      #
    # For TrueNAS iSCSI the target/extent lifecycle is fully managed via  #
    # the TrueNAS REST API in initialize_connection/terminate_connection.  #
    # These Cinder export hooks are deliberate no-ops.                    #
    # ------------------------------------------------------------------ #

    def create_export(self, context, volume, connector):
        """No-op: TrueNAS targets are created on-demand in initialize_connection."""
        LOG.debug('iXsystems: create_export (no-op) for %s', volume.name)
        return {}

    def ensure_export(self, context, volume):
        """No-op: TrueNAS targets are verified on-demand in initialize_connection."""
        LOG.debug('iXsystems: ensure_export (no-op) for %s', volume.name)

    def remove_export(self, context, volume):
        """
        No-op: TrueNAS target/extent teardown happens in terminate_connection.

        Cinder calls remove_export() during delete_volume() to clean up any
        persistent iSCSI export. For TrueNAS we manage targets entirely via
        REST API calls in terminate_connection(), so nothing to do here.
        """
        LOG.debug('iXsystems: remove_export (no-op) for %s', volume.name)

    # ------------------------------------------------------------------ #
    # iSCSI attachment                                                     #
    # ------------------------------------------------------------------ #

    def initialize_connection(self, volume, connector):
        """
        Attach volume to an initiator.

        Creates the iSCSI target + extent on TrueNAS (or reuses existing ones
        for multi-attach volumes) and returns the connection properties that
        Nova/os-brick uses to perform the iSCSI login on the compute node.

        For multi-attach volumes this is called once per VM — the same iSCSI
        target is reused; only the LUN mapping is idempotent.
        """
        LOG.info('iXsystems: initialize_connection %s initiator=%s',
                 volume.name, connector.get('initiator'))

        target, extent, lun_id = self.common._create_target_and_extent(volume.name)

        # Build full IQN: basename + short volume name.
        # _get_iscsi_target_name() returns the short name only (volume.name).
        # TrueNAS stores targets by short name; ctld prepends the global
        # basename at runtime. We reconstruct the full IQN here for Nova/os-brick.
        global_config = self.common._get_iscsi_global_config()
        basename = global_config.get(
            'basename', self.configuration.ixsystems_iqn_prefix
        ).rstrip(':')
        iqn = f'{basename}:{volume.name}'

        portal = f'{self.configuration.ixsystems_server_hostname}:3260'

        properties = {
            'target_discovered': False,
            'target_iqn': iqn,
            'target_portal': portal,
            'volume_id': volume.id,
            'target_lun': lun_id,
            'access_mode': 'rw',
        }

        LOG.info('iXsystems: connection properties for %s: iqn=%s portal=%s lun=%s',
                 volume.name, iqn, portal, lun_id)
        return {'driver_volume_type': 'iscsi', 'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        """
        Detach a volume from an initiator.

        Multi-attach aware: only tears down the iSCSI target and extent on
        TrueNAS when this is the LAST remaining attachment. If other VMs still
        have the volume attached the target is kept alive.

        The connector is None when Cinder forces a detach (e.g. during volume
        deletion or when a compute node is lost). In that case we always tear
        down regardless of remaining attachment count.
        """
        LOG.info('iXsystems: terminate_connection %s connector=%s',
                 volume.name, connector)

        # Force-detach path (connector is None): always clean up
        if connector is None:
            LOG.info(
                'iXsystems: force detach for %s — removing target unconditionally',
                volume.name
            )
            self.common._remove_target_and_extent(volume.name)
            return

        # Multi-attach check: count attachments that are NOT this connector.
        # volume.volume_attachment is a list of VolumeAttachment objects.
        # Each has a .connector dict identifying the attached host.
        try:
            all_attachments = volume.volume_attachment
            remaining = [
                att for att in all_attachments
                if att.connector and att.connector != connector
            ]
            remaining_count = len(remaining)
        except Exception as e:
            # If we can't determine attachment count, be safe and keep target
            LOG.warning(
                'iXsystems: could not determine remaining attachments for %s '
                '(%s) — keeping target intact to be safe',
                volume.name, e
            )
            return

        if remaining_count > 0:
            LOG.info(
                'iXsystems: %s still has %d active attachment(s) after this '
                'detach — keeping iSCSI target intact on TrueNAS',
                volume.name, remaining_count
            )
            return

        # This is the last attachment — safe to remove target and extent
        LOG.info(
            'iXsystems: last attachment removed for %s — '
            'tearing down iSCSI target and extent on TrueNAS',
            volume.name
        )
        self.common._remove_target_and_extent(volume.name)

    # ------------------------------------------------------------------ #
    # Stats                                                                #
    # ------------------------------------------------------------------ #

    def get_volume_stats(self, refresh=False):
        self._init_common()
        if refresh or not self.common.stats:
            LOG.info('iXsystems Get Volume Status')
            self.stats = self.common._update_volume_stats()
        return self.common.stats
