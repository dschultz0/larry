#!/usr/bin/env bash

./update_readme.sh
rm dist/*
rm -r build
python3 setup.py bdist_wheel
python3 -m twine upload dist/*