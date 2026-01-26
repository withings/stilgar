#!/bin/bash

clickhouse-client -n <<-EOF
CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB
EOF
true
