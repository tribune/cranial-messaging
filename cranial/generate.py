#! /usr/bin/python3
"""
Usage:
    generate [<type>]

    Possible types are:
        service: generate a receiving listener with a custom module and function
                which processes bytes from the specified listener
"""

from docopt import docopt
from cookiecutter.main import cookiecutter
import os

opts = docopt(__doc__)

if opts.get('<type>') == 'service':
    # todo better way to get to the template
    cookiecutter(os.getcwd() + '/cranial/cli/service_template')
else:
   print('Only types that can be generated are: service')
