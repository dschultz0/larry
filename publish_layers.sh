#!/usr/bin/env bash

mkdir -p layer/python
cp -R larry layer/python

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


base_version_west_2=$(aws lambda publish-layer-version --layer-name "Larry" --region "us-west-2" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" "python3.8" \
  --zip-file "fileb://layer.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


jinja_version_west_2=$(aws lambda publish-layer-version --layer-name "LarryWithJinja" --region "us-west-2" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services (includes Jinja2)." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" "python3.8" \
  --zip-file "fileb://layerjinja.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


pillow_version_west_2=$(aws lambda publish-layer-version --layer-name "LarryWithJinjaPillow" --region "us-west-2" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services. (includes Jinja2 and Pillow)" \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7"  "python3.8" \
  --zip-file "fileb://layerjinjapillow.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")

aws lambda add-layer-version-permission --layer-name "Larry" --region "us-west-2" \
--statement-id public --version-number $base_version_west_2 --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryWithJinja" --region "us-west-2" \
--statement-id public --version-number $jinja_version_west_2 --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryWithJinjaPillow" --region "us-west-2" \
--statement-id public --version-number $pillow_version_west_2 --principal '*' \
--action lambda:GetLayerVersion

base_version_east_1=$(aws lambda publish-layer-version --layer-name "Larry" --region "us-east-1" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" "python3.8" \
  --zip-file "fileb://layer.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


jinja_version_east_1=$(aws lambda publish-layer-version --layer-name "LarryWithJinja" --region "us-east-1" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services (includes Jinja2)." \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7" "python3.8" \
  --zip-file "fileb://layerjinja.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")


pillow_version_east_1=$(aws lambda publish-layer-version --layer-name "LarryWithJinjaPillow" --region "us-east-1" \
  --description "Library of useful utilities for speeding up data manipulation in S3, MTurk, and other AWS services. (includes Jinja2 and Pillow)" \
  --license-info "MIT" \
  --compatible-runtimes "python3.6" "python3.7"  "python3.8" \
  --zip-file "fileb://layerjinjapillow.zip" | python3 -c "import sys, json; print(json.load(sys.stdin)['Version'])")

aws lambda add-layer-version-permission --layer-name "Larry" --region "us-east-1" \
--statement-id public --version-number $base_version_east_1 --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryWithJinja" --region "us-east-1" \
--statement-id public --version-number $jinja_version_east_1 --principal '*' \
--action lambda:GetLayerVersion

aws lambda add-layer-version-permission --layer-name "LarryWithJinjaPillow" --region "us-east-1" \
--statement-id public --version-number $pillow_version_east_1 --principal '*' \
--action lambda:GetLayerVersion


FILE="./layer_versions.json"

/bin/cat <<EOM >$FILE
{
  "us_east_1_base": $base_version_east_1,
  "us_east_1_jinja": $jinja_version_east_1,
  "us_east_1_pillow": $pillow_version_east_1,
  "us_west_2_base": $base_version_west_2,
  "us_west_2_jinja": $jinja_version_west_2,
  "us_west_2_pillow": $pillow_version_west_2
}
EOM

echo "Published versions $base_version_east_1, $jinja_version_east_1, $pillow_version_east_1, $base_version_west_2, $jinja_version_west_2, $pillow_version_west_2"

rm -r layer
rm layer.zip
rm layerjinja.zip
rm layerjinjapillow.zip

./update_readme.sh
