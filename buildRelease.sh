#!/bin/bash
usage(){
cat<<EOF

    Tag & build a release from a git repository
    Usage:
      buildRelease.sh [options]

    options:
    -r <remote>       Remote repository to update, default to origin.
    -b <gitbranch>    Git branch to be tagged, default to master.
    -t <tag>          Tag name to be created.
    -e <tagRegExp>    A regular expresion to describe the tag naming schema
    -n                Dry run mode. No actions would be performed on the reposiory
    -h                Show this help.


EOF
}

set -e

# Set defaults:
REMOTE='origin'
BRANCH='master'
TAG=''
TAGREGEXP="v?[0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)*$"
DRYRUN=false

while getopts ":r:b:t:e:hn" opt; do
    case ${opt} in
        r)
            REMOTE=$OPTARG
            REPOURL=`git remote get-url $REMOTE`
            ;;
        b)
            BRANCH=$OPTARG ;;
        t)
            TAG=$OPTARG ;;
        e)
            TAGREGEXP=$OPTARG ;;
        n)
            DRYRUN=true ;;
        h)
            usage; exit 0 ;;
        \? )
            echo -e "\nERROR: Invalid Option: -$OPTARG\n"
            usage
            exit 1 ;;
        : )
            echo -e "\nERROR: Invalid Option: -$OPTARG requires an argument\n"
            usage
            exit 1 ;;
    esac
done
# shift to the last parsed option, so we can consume the non-parametric
# arguments passed to the call with a regular shift
# shift $(expr $OPTIND - 1 )

# Check if TAG was provided;
[[ -z $TAG ]] && { usage ; echo "ERROR: No TAG provided"; exit 3 ;}

# Check if the TAG provided follows the tag regular expression:
[[ $TAG =~ $TAGREGEXP ]] || { echo "ERROR: TAG:$TAG does not match TAGREGEXP: $TAGREGEXP"; exit 4 ;}

# Check if remote exists, and create it if it is missing:
[[ `git remote`  =~ $REMOTE ]] || {
    echo "ERROR: Missing remote: $REMOTE."
    exit 2
}

# Find the final real repoUrl and protocol
repoUrl=`git remote get-url $REMOTE`
repoUrlProto=${repoUrl%%:*}
repoUrlProto=${repoUrlProto%%@*}

echo
echo "======================================================="
echo "You are about to create:"
echo "RELEASE   : $TAG"
echo "REPOSITORY: $repoUrl"
echo
echo -n "Continue? [y]: "
read x && [[ $x =~ (n|no|nO|N|No|NO) ]] && exit 101
echo "-------------------------------------------------------"
echo

# Find the current repository name:
repoName=`echo $repoUrl | xargs basename -s .git`
echo repoName: $repoName

# Find the current repository owner:
repoOwner=${repoUrl##*github.com:}
repoOwner=${repoOwner##*github.com/}
repoOwner=${repoOwner%%/$repoName*}
echo repoOwner: $repoOwner

# Build the current repository api url
repoApiUrl="https://api.github.com/repos/$repoOwner/$repoName"
echo repoApiUrl: $repoApiUrl

# Check whether we are in the correct directory
[[ $(git rev-parse --show-toplevel) == $(realpath $PWD) ]] || {
  echo "ERROR: Not in root directory $(git rev-parse --show-toplevel)!"
  exit 5
}

# Check if tag exists
git show-ref --tags --quiet -- $TAG && {
  echo "ERROR: Tag $TAG exists!"
  exit 6
}

echo "Checking out branch: $BRANCH"
git checkout $BRANCH

echo "Pulling from upstream"
git pull $REMOTE $BRANCH
git fetch $REMOTE

# Fetch the last tag commit (hash id plus tag name)
lastRelCommitLine=$(git log -n1 --oneline --no-decorate -E --grep=$TAGREGEXP)
lastRelCommit=$(echo ${lastRelCommitLine} | awk '{print $1}')
lastVersion=$(echo ${lastRelCommitLine} | awk '{print $2}')

# If no dedicated release commit is found, just take the last tag created.
[[ -n $lastRelCommit ]] || {
    lastTag=$(git describe --long --always --tags)
    lastTag=${lastTag%%-*}
    lastTagDescribe=$(git describe --long --always --tags $lastTag)
    lastVersion=${lastTagDescribe%%-*}
    lastRelCommit=${lastTagDescribe##*-g}
}

echo lastRelCommitLine: $lastRelCommitLine
echo lastTagDescribe: $lastTagDescribe
echo lastRelCommit: $lastRelCommit
echo lastVersion: $lastVersion

# Generating the CHANGES file
echo "Generating CHANGES file ..."
changesFile=CHANGES.md
changesFileTmp=$(mktemp -t $repoName.${TAG}.XXXXX)

echo "### **${lastVersion} to ${TAG}:**" >> $changesFileTmp

# Grab all the commit hashes, subject and author since the last release
changesCommitsFile=${changesFileTmp}.commits
git log --no-merges  --pretty=format:'%H %s (%aN)' ${lastRelCommit}.. >> $changesCommitsFile
echo "" >> $changesCommitsFile

echo changesFileTmp: $changesFileTmp
echo changesCommitsFile: $changesCommitsFile

# Use github public API to fetch pull request # from commit hash
cat $changesCommitsFile | while read commitLine; do
    [[ -z "$commitLine" ]] && continue # line is empty
    hashId=$(echo $commitLine | awk '{print $1}')
    # remove hash id from the commit line
    commitLine=$(echo $commitLine | sed "s/$hashId/  -/")
    # NOTE: If we do not find a proper  Pull Request Url it must have been a commit
    #       directly to a branch. Then we must populate the branch name instead of prUrl
    prUrl=$(curl -s $repoApiUrl/commits/$hashId/pulls | jq -r '.[]["html_url"]')
    if [[ -n $prUrl ]]; then
        prNum=`basename $prUrl`
        echo "$commitLine [#$prNum]($prUrl)" >> $changesFileTmp
    else
        commitUrl=$(curl -s $repoApiUrl/commits/$hashId | jq -r '.["html_url"]')
        commitBranch=$(curl -s $repoApiUrl/commits/$hashId/branches-where-head | jq -r '.[]["name"]')
        echo "$commitLine [${hashId::6}]($commitUrl) on $commitBranch" >> $changesFileTmp
    fi
done
echo -en '\n\n' >> $changesFileTmp

# Append the original CHANGES content and later swap the files if not in DRYRUN mode
[[ -f $changesFile ]] && cat $changesFile >> $changesFileTmp

${EDITOR:-vi} $changesFileTmp || {
    echo "ERROR: User canceled CHANGES update"
    exit 7
}

# Apply all the changes to the local tree and tag
echo
echo "======================================================="
echo "These are the commits to be included in the tag: $TAG:"
git log --pretty=format:'  - %s' ${lastRelCommit}..
echo

# Exit here if we are in DRYRUN mode. The rest of the script is intrusive to the local tree
$DRYRUN && exit 0

echo -n "Continue? [n]: "
read x && [[ $x =~ (y|Y|yes|Yes|YES) ]] || { echo "ERROR: User interrupt"; exit 102 ;}
echo "-------------------------------------------------------"
echo

echo "Saving the new CHANGES file ..."
cp $changesFileTmp $changesFile
git add $changesFile

echo "Creating a tag commit ..."
git commit -a -s -m "$TAG"

echo "Tagging release ..."
git log --pretty=format:'  - %s' ${lastRelCommit}.. | git tag -a $TAG -F -

echo "Pushing to ${REMOTE} ..."
git push --tags ${REMOTE} ${BRANCH}
set +e

echo "$TAG tagged"

echo

exit 0
