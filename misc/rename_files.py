
import os
import os.path
import sys
import re

dir = '/Users/spyros/Music/DJ Library/salsa 2026-01'

def main():
    for file in os.listdir(dir):
        m = re.match(r'^([0-9-]+ - )(.*\.aiff?)$', file)
        if m:
            new_file = m.group(2)
            full_file = os.path.join(dir, file)
            full_new_file = os.path.join(dir, new_file)
            os.rename(full_file, full_new_file)
    return

if __name__ == '__main__':
    main()
    sys.exit(0)
