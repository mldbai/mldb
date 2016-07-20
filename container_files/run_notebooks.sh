#!/bin/bash

for nbdir in _tutorials _demos _examples
do
    cd /mldb_data/$nbdir/_latest
    for f in *.ipynb
    do
        time jupyter nbconvert  --config /{{IPYTHON_DIR}}/nbconvert_cfg.py --to notebook --execute --inplace "$f"
    done
done
