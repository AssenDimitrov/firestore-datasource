## This is hard fork of pgollangi/firestore-grafana-datasource
https://github.com/pgollangi/firestore-grafana-datasource
<br>
I do not own the original code, I was just frustrated how outdated the Firestore grafana datasource is, hence implemented some changes myself.

## Why I did this?
Because the original pgollangi/firestore-grafana-datasource does not support:
- Named databases
- Inconsistent document attributes
- Doesnt return document ID

## How to use
There are prebuilt plugin packages in ./dist for Grafana for amd64 arch.
In case you need another arch you have to build it yourself with:
```bash
(yarn needs nodejs 18)
yarn install
yarn build

go clean --modcache
go mod tidy     
go mod download
go mod verify

go build -o dist/gpx_firestore_linux_amd64 ./pkg
```

## Imporotant
The datasource relies on FireQL, another implementation of pgollangi which also needed some modifications
https://github.com/pgollangi/FireQL
<br>
Therefore, this repo also has ./FireQL folder which contains the modified lib.

