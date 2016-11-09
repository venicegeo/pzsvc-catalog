#! /bin/bash -ex

pushd `dirname $0`/.. > /dev/null
root=$(pwd -P)
popd > /dev/null

export GOPATH=$root/gopath

source $root/ci/vars.sh

mkdir -p $GOPATH $GOPATH/bin $GOPATH/src $GOPATH/pkg

PATH=$PATH:$GOPATH/bin

go version

# install metalinter
go get -u github.com/alecthomas/gometalinter
gometalinter --install

go get -v github.com/venicegeo/pzsvc-image-catalog/...

cd $GOPATH/src/github.com/venicegeo/pzsvc-image-catalog

# run unit tests w/ coverage collection
go test -v -coverprofile=catalog.cov github.com/venicegeo/pzsvc-image-catalog/catalog
go test -v -coverprofile=cmd.cov github.com/venicegeo/pzsvc-image-catalog/cmd
cp ./catalog.cov $root/catalog.cov
cp ./cmd.cov $root/cmd.cov


# lint
gometalinter \
--deadline=60s \
--concurrency=6 \
--vendor \
--exclude="exported (var)|(method)|(const)|(type)|(function) [A-Za-z\.0-9]* should have comment" \
--exclude="comment on exported function [A-Za-z\.0-9]* should be of the form" \
--exclude="Api.* should be .*API" \
--exclude="Http.* should be .*HTTP" \
--exclude="Id.* should be .*ID" \
--exclude="Json.* should be .*JSON" \
--exclude="Url.* should be .*URL" \
--exclude="[iI][dD] can be fmt\.Stringer" \
--exclude=" duplicate of [A-Za-z\._0-9]*" \
./... | tee $root/lint.txt
wc -l $root/lint.txt

# gather some data about the repo

cd $root
cp $GOPATH/bin/$APP ./$APP.bin
tar cvzf $APP.$EXT \
    $APP.bin \
    cmd.cov \
	catalog.cov \
    lint.txt
tar tzf $APP.$EXT


#src=$GOPATH/bin/pzsvc-image-catalog
#mv $src $root/$APP.$EXT

#go test -v -coverprofile=geojson.cov github.com/venicegeo/geojson-go/geojson
#go test -v -coverprofile=catalog.cov github.com/venicegeo/pzsvc-image-catalog
