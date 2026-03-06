FROM registry.redhat.io/rhoso/openstack-cinder-volume-rhel9:18.0

USER root

# Find the correct site-packages path at build time and copy there
RUN PYPATH=$(python3 -c "import site; print(site.getsitepackages()[0])") && \
    echo "Installing to: $PYPATH" && \
    mkdir -p $PYPATH/cinder/volume/drivers/ixsystems

COPY driver/ixsystems/ /tmp/ixsystems/

RUN PYPATH=$(python3 -c "import site; print(site.getsitepackages()[0])") && \
    cp -R /tmp/ixsystems/. $PYPATH/cinder/volume/drivers/ixsystems/ && \
    rm -rf /tmp/ixsystems

USER cinder
