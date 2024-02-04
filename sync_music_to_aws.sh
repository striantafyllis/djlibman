#!/bin/bash

for dir in 'personal' 'recordings' 'MUS 290' 'MUS 291' 'PioneerDJ' 'Old Music'
do
  echo "Syncing $dir"
  echo aws s3 sync --delete ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
  time aws s3 sync --delete ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
done
