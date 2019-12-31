#!/usr/bin/env bash

versions=$(<layer_versions.json)
echo "$versions"

python -c "import jinja2; import json; print(jinja2.Template(open('readme_template.j2.md').read()).render(layers=$versions))" > README.md

