# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import re
import sys

# del the dir path
sys.path = sys.path[1:]
from tensorboard.main import run_main

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(run_main())



