#!/usr/bin/env bash

export PYTHONPATH=../:../../
pythonenv="/usr/bin/python"


echo "update_community_post"

${pythonenv} index_part2_community_review.py

echo "done"
