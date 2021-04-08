# openebs
Chart is forked from [Original Source](https://github.com/openebs/charts/tree/master/charts/openebs)

### Prerequisite
1) kubectl should be installed and configured in machine from which deployment will be made
2) helm 3 should be installed and configured
3) nodes should be assigned label ```nodeType=storage```


## Installation
#### Command
```bash
$ make install
$ # or
$ make install ns=openebs-ls release=openebs-deployment
```
#### Command args
1) **ns**=yournamespace
2) **chart**=desriredoperatorchart
3) **lpath**=localpath
4) **release**=name
5) **valuesFile**=filename

## Uninstallation
```bash
$ make uninstall
```
## make targets
1) **clean**: Cleans the local fs
2) **install**: install the chart
3) **uninstall**: uninstalls the chart
4) **rke-yaml**: generate the deployment yamls
5) **createns**: create namespace
6) **removens**: delete namespace

