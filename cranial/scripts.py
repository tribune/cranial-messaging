import sys
import subprocess


def cranial():  # python 3.7+, otherwise define each script manually
    cmd_args = sys.argv[2:] if len(sys.argv) > 2 else []
    subprocess.run(
        ['python', '-O', '-u', '-m', 'cranial.'+ sys.argv[1]] + cmd_args
    )  # run whatever you like based on 'name'
