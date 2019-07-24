import os
import sys

p = os.path.abspath('..')
sys.path.append(p)

try:
    import {{ cookiecutter.module }}
except Exception as e:
    print('Could not find module. Is module in the current directory?', 'Error: ', e)
    sys.exit(1)