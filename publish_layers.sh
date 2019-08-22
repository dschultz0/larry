#!/usr/bin/env bash

mkdir -p layer/python
cp -R larrydata layer/python

cd layer
zip ../layer.zip -r ./python/
cd ..

pip install Jinja2 -t layer/python
cd layer
zip ../layerjinja.zip -r ./python/
cd ..

pip install Pillow -t layer/python
cd layer
zip ../layerjinjapillow.zip -r ./python/
cd ..


aws lambda publish-layer-version --layer-name "LarryData" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layer.zip"


aws lambda publish-layer-version --layer-name "LarryDataWithJinja" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services (includes Jinja2)." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layerjinja.zip"


aws lambda publish-layer-version --layer-name "LarryDataWithJinjaPillow" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services. (includes Jinja2 and Pillow)" \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layerjinjapillow.zip"

rm -r layer
rm layer.zip
rm layerjinja.zip
rm layerjinjapillow.zip

