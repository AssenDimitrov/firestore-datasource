#!/usr/bin/bash
rm -f dist/gpx_firestore_linux_amd64
go build -o dist/gpx_firestore_linux_amd64 ./pkg || exit 1
sudo docker build . -t grafana-custom:1
sudo docker run -p 3000:3000 --network host -e GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=custom-firestore-datasource grafana-custom:1
