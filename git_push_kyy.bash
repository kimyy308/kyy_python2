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

# Check current status
git status

# Pull remote updates first
git pull --rebase ${reponame} ${branch}

# Add changes
git add -A

# Commit only if there is something to commit
if git diff --cached --quiet; then
    echo "No staged changes to commit."
else
    time_now=`date "+%Y%m%d %H:%M:%S"`
    git commit -m "${time_now}"
fi

# Push
git push -u ${reponame} ${branch}

git status
