#!/bin/sh

mvn clean package -Dmaven.test.skip=true

if [ -d "./lib/" ]; then
   rm -r ./lib/
fi

mkdir ./lib/

cp ./integration-parent/tsfile-hadoop/target/lib/*.jar ./lib/
cp ./tsfile-format/target/lib/*.jar ./lib/
cp ./tsfile-impl-parent/tsfile-common/target/lib/*.jar ./lib/
cp ./tsfile-impl-parent/tsfile-compression/target/lib/*.jar ./lib/
cp ./tsfile-impl-parent/tsfile-encoding/target/lib/*.jar ./lib/
cp ./tsfile-impl-parent/tsfile-file/target/lib/*.jar ./lib/
cp ./tsfile-impl-parent/tsfile-timeseries/target/lib/*.jar ./lib/
