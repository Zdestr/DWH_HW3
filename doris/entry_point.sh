#!/bin/bash
set -e

if [ ! -f /opt/apache-doris/be/conf/be.conf ]; then
  echo "ERROR: be.conf not found at /opt/apache-doris/be/conf/be.conf"
  exit 1
fi

/opt/apache-doris/be/bin/start_be.sh

tail -f /opt/apache-doris/be/log/be.out
