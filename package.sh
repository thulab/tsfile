#!/bin/sh

mvn clean package -Dmaven.test.skip=true

if [ -d "./lib/" ]; then
   rm -r ./lib/
fi

mkdir ./lib/

cp ./target/lib/*.jar ./lib/

