#!/bin/sh
if [ ! -f /var/lib/nvme/hostid ]; then
  uuidgen > /var/lib/nvme/hostid
fi
cp /var/lib/nvme/hostid /etc/nvme/hostid
echo "nqn.2014-08.org.nvmexpress:uuid:$(cat /etc/nvme/hostid)" > /etc/nvme/hostnqn

exec /usr/local/bin/spdkcsi
