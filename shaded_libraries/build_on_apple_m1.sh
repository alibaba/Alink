#!/usr/bin/env bash

set -ex

# download protoc binaries
wget https://repo1.maven.org/maven2/com/google/protobuf/protoc/3.8.0/protoc-3.8.0-osx-x86_64.exe
wget https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.13.2/protoc-gen-grpc-java-1.13.2-osx-x86_64.exe

chmod +x protoc-3.8.0-osx-x86_64.exe
chmod +x protoc-gen-grpc-java-1.13.2-osx-x86_64.exe

mvn -Dprotoc-command=`pwd`/protoc-3.8.0-osx-x86_64.exe \
    -Dprotoc-gen-grpc-command=`pwd`/protoc-gen-grpc-java-1.13.2-osx-x86_64.exe \
    -Dmaven.test.skip=true \
    -DskipTests=true \
    -Prelease clean package
