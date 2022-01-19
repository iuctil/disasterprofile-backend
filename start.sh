#!/bin/bash

set -ex

pm2 delete api || true

cd api && pm2 start ts-node --name api -- --type-check server.ts --watch
