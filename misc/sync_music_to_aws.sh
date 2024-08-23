#!/bin/bash

for dir in 'personal' 'recordings' 'MUS 290' 'MUS 291' 'MUS 292' 'MUS 131' 'PioneerDJ' 'Old Music' 'edits' 'djlib'
do
  echo "Syncing $dir"
  echo aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
  time aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
done
