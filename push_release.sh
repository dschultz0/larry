#!/usr/bin/env bash

./update_readme.sh
rm dist/*
rm -r build
python setup.py bdist_wheel
python -m twine upload dist/*