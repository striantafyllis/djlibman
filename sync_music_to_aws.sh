#!/bin/bash

for dir in 'personal' 'recordings' 'MUS290' 'PioneerDJ'
do
  echo "Syncing $dir"
  echo aws s3 sync --delete ~/Music/$dir s3://spyros-personal2/Music/$dir
  time aws s3 sync --delete ~/Music/$dir s3://spyros-personal2/Music/$dir
done
