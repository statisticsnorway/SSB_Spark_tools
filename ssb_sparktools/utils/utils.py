import os
import sys

def set_runpath(repo):
    repo = '/' + repo
    if os.getcwd()[:13] =='/home/runner/':
        os.chdir(os.getcwd()[:(os.getcwd().index(repo) + ((len(repo)*2)+1))] )
    else:
        os.chdir(os.getcwd()[:(os.getcwd().index(repo) + ((len(repo))+1))] )
    return os.getcwd()