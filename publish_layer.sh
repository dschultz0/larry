#!/usr/bin/env bash

mkdir layer
mkdir layer/python
cp -R larrydata layer/python

rm layer.zip
cd layer
zip ../layer.zip -r ./python/
cd ..
rm -r layer

aws lambda publish-layer-version --layer-name "LarryData" \
  --description "Library of useful utilities for speeding up data manipulation in S3 and other AWS services." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layer.zip"

