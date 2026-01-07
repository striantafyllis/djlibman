
import os
import os.path
import sys
import re

source_dir = '/Users/spyros/Downloads/qobuz'
dest_dir = '/Users/spyros/Music/DJ Library/salsa 2026-01'
file_regex = re.compile('^.*\.aiff?', re.IGNORECASE)

def descend(dir):
    assert os.path.isdir(dir)

    retval = []

    for file in os.listdir(dir):
        if file.startswith('.'):
            continue

        full_file = os.path.join(dir, file)
        if os.path.isdir(full_file):
            retval += descend(full_file)
        elif re.match(file_regex, file):
            retval += [full_file]

    return retval

def main():
    files = descend(source_dir)

    for file in files:
        new_file = os.path.join(dest_dir, os.path.basename(file))
        print(f'{file} -> {new_file}')
        os.rename(file, new_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
