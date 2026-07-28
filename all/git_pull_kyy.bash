#!/bin/bash

account='kimyy308'
repo='kyy_python2' # Remote repository name you want to pull
reponame='origin' # Bookmark for the remote repository
branch='main' # Branch name you want to pull

rm -rf ./.git

git init
git config --global user.name ${account}
git config --global user.email ${account}@snu.ac.kr
git remote add ${reponame} https://github.com/${account}/${repo}.git
git remote set-url ${reponame} https://${account}@github.com/${account}/${repo}.git
git pull ${reponame} ${branch}
