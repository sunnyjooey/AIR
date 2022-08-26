#!/usr/bin/env bash
rm -r src/insults/tests/
find . -type d -name "__pycache__" -exec rm -R {} +;
find . -type d -name ".pytest_cache" -exec rm -R {} +;
pytest . --home_rootdir . --color="yes"
