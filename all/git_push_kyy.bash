#!/bin/bash
set -e

account='kimyy308'
repo='kyy_python2'
reponame='origin'
branch='main'
file2upload='all'

git config user.name "${account}"
git config user.email "${account}@snu.ac.kr"

git remote set-url ${reponame} https://${account}@github.com/${account}/${repo}.git

git checkout ${branch}

# Add only the "all" directory
git add -A -- ${file2upload}

# Commit only if there are changes in all/
if git diff --cached --quiet; then
    echo "No changes in ${file2upload} to commit."
else
    time_now=`date "+%Y%m%d %H:%M:%S"`
    git commit -m "${time_now}"
fi

# Push local main to remote main
git push -u ${reponame} ${branch}
