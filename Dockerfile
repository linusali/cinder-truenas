FROM registry.redhat.io/rhoso/openstack-cinder-volume-rhel9:18.0

USER root

# Find the correct site-packages path at build time and copy there
RUN PYPATH="/usr/lib/python3.9/site-packages" && \
    echo "Installing to: $PYPATH" && \
    mkdir -p $PYPATH/cinder/volume/drivers/ixsystems

COPY driver/ixsystems/ /tmp/ixsystems/

RUN PYPATH="/usr/lib/python3.9/site-packages" && \
    cp -R /tmp/ixsystems/. $PYPATH/cinder/volume/drivers/ixsystems/ && \
    rm -rf /tmp/ixsystems

USER cinder
