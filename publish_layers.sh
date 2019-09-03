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


base_version=$(aws lambda publish-layer-version --layer-name "LarryData" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layer.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


jinja_version=$(aws lambda publish-layer-version --layer-name "LarryDataWithJinja" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services (includes Jinja2)." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layerjinja.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


pillow_version=$(aws lambda publish-layer-version --layer-name "LarryDataWithJinjaPillow" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services. (includes Jinja2 and Pillow)" \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" \
  --zip-file "fileb://layerjinjapillow.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")

echo "Published versions $base_version, $jinja_version, $pillow_version"

aws lambda add-layer-version-permission --layer-name "LarryData" \
--statement-id public --version-number $base_version --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryDataWithJinja" \
--statement-id public --version-number $jinja_version --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryDataWithJinjaPillow" \
--statement-id public --version-number $pillow_version --principal '*' \
--action lambda:GetLayerVersion

python -c "import jinja2; print(jinja2.Template(open('readme_template.j2.md').read()).render(layer_version=$base_version, layer_version_jinja=$jinja_version, layer_version_pillow=$pillow_version))" > README.md

rm -r layer
rm layer.zip
rm layerjinja.zip
rm layerjinjapillow.zip

