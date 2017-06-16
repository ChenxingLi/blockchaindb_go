#!/bin/bash

./prepare.sh
cd server
go build
cp server ../cross-test/server
