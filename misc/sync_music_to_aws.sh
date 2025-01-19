#!/bin/bash

for dir in 'personal' 'recordings' 'Old Classes' 'MUS 112' 'MUS 132' 'PioneerDJ' 'Old Music' 'edits' 'djlib' 'projects'
do
  echo "Syncing $dir"
  echo aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
  time aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
done
