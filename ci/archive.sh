#! /bin/bash -ex

sudo apt-get install -y libgeos-dev

pushd `dirname $0`/.. > /dev/null
root=$(pwd -P)
popd > /dev/null

export GOPATH=$root/gopath
mkdir -p $GOPATH
source $root/ci/vars.sh

go get -v github.com/venicegeo/pzsvc-image-catalog/...

src=$GOPATH/bin/pzsvc-image-catalog
mv $src $root/$APP.$EXT
