#!/bin/bash

for dir in 'DJ Library' 'DJ publicity' 'recordings' 'Old Classes' 'Classes' 'Pyramind Ableton Class' 'PioneerDJ' 'Old Music' 'edits' 'djlib' 'projects' 'loops'
do
  echo "Syncing $dir"
  echo aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
  time aws s3 sync --delete --exclude '.DS_Store' ~/Music/"$dir" s3://spyros-personal2/Music/"$dir"
done
