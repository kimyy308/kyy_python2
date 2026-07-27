#!/bin/bash
set -e

account='kimyy308'
repo='kyy_python2'
reponame='origin'
branch='main'

git config user.name "${account}"
git config user.email "${account}@snu.ac.kr"

git remote set-url ${reponame} https://${account}@github.com/${account}/${repo}.git

git checkout ${branch}
git pull --rebase ${reponame} ${branch}
git status
