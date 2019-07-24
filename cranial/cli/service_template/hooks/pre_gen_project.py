import sys
import importlib

# Todo check if function exists

module= '{{ cookiecutter.module }}'

try:
    mod = importlib.import_module(module)
except Exception as e:
    print('Could not import module.', 'Error: ', e)
    sys.exit(1)