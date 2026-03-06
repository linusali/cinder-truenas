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
                urllib.request replacing urllib2; stdlib json replacing simplejson
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
        self.common._create_cloned_volume(volume.name, src_vref.name, volume.size)

    # ------------------------------------------------------------------ #
    # iSCSI attachment                                                     #
    # ------------------------------------------------------------------ #

    def initialize_connection(self, volume, connector):
        LOG.info('iXsystems: initialize_connection %s initiator=%s',
                 volume.name, connector.get('initiator'))

        target, extent, lun_id = self.common._create_target_and_extent(volume.name)

        global_config = self.common._get_iscsi_global_config()
        basename = global_config.get(
            'basename', self.configuration.ixsystems_iqn_prefix
        )
        target_name = self.common._get_iscsi_target_name(volume.name)
        iqn = target_name if basename.rstrip(':') in target_name \
            else f'{basename.rstrip(":")}:{target_name}'

        portal = f'{self.configuration.ixsystems_server_hostname}:3260'

        properties = {
            'target_discovered': False,
            'target_iqn': iqn,
            'target_portal': portal,
            'volume_id': volume.id,
            'target_lun': lun_id,
            'access_mode': 'rw',
        }
        return {'driver_volume_type': 'iscsi', 'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        LOG.info('iXsystems: terminate_connection %s', volume.name)
        self.common._remove_target_and_extent(volume.name)

    # ------------------------------------------------------------------ #
    # Stats                                                                #
    # ------------------------------------------------------------------ #

    def get_volume_stats(self, refresh=False):
        LOG.info('iXsystems Get Volume Status')
        self._init_common()
        self.stats = self.common._update_volume_stats()
        return self.stats
