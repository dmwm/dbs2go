#!/bin/bash

# ----------------------------------------------------------------------
# dbs2go Megalodon CI incident evidence preservation + master cleanup
#
# Goal:
# - preserve the incident chain as one passive git bundle file
# - store that bundle on one orphan evidence branch
# - keep master history clean
# - create no tags
# - create no cleanup branch
# - create no branch pointing directly to the malicious commit chain
# - do not decode or execute the malicious payload
# - do not push anything upstream until the final manual section
# ----------------------------------------------------------------------

REMOTE=upstream
BASE=master

PRE_BAD=964e2e45e510af2852d1e5ff4e89018a991490d5
BAD=be9af07e64f12518455308cfde7749bfa6c5fd1d
REVERT=73ac01f971eb143ec2226ee9a7b07a75534b4e85
MERGE_REVERT=a626518253526586b23933e7f3f61e5fbcb2866c

SHORT=${BAD:0:7}

EVIDENCE_BRANCH="incident/passive-evidence-megalodon-ci-${SHORT}"

BUNDLE_NAME="dbs2go-megalodon-ci-incident-chain-${SHORT}.bundle"
BUNDLE_TMP="../${BUNDLE_NAME}"

# ----------------------------------------------------------------------
# 1. Inspect local state
# ----------------------------------------------------------------------

git status --short

# Stop manually if this shows local modifications you want to keep.

git fetch --all --prune || exit 1

git cat-file -e "$PRE_BAD^{commit}" || exit 1
git cat-file -e "$BAD^{commit}" || exit 1
git cat-file -e "$REVERT^{commit}" || exit 1
git cat-file -e "$MERGE_REVERT^{commit}" || exit 1

git show-ref --verify --quiet "refs/heads/$EVIDENCE_BRANCH" && {
  echo "ERROR: evidence branch already exists:"
  echo "  $EVIDENCE_BRANCH"
  echo
  echo "Inspect it manually before retrying:"
  echo "  git log --graph --decorate --oneline $EVIDENCE_BRANCH --max-count=20"
  echo
  echo "If you are sure it is safe to remove:"
  echo "  git branch -D $EVIDENCE_BRANCH"
  echo
  echo "Then rerun the procedure."
  exit 1
}

# ----------------------------------------------------------------------
# 2. Create exactly one incident evidence bundle
# ----------------------------------------------------------------------
#
# This must be done before cleaning master.
#
# Current local master is expected to point to:
#
#   a626518  merge commit for revert PR
#
# The range PRE_BAD..master preserves:
#
#   be9af07  malicious commit
#   73ac01f  revert commit
#   a626518  merge commit for revert PR

git bundle create "$BUNDLE_TMP" "$PRE_BAD..master" || exit 1

git bundle verify "$BUNDLE_TMP" || exit 1

# ----------------------------------------------------------------------
# 3. Create orphan evidence branch containing only the passive bundle
# ----------------------------------------------------------------------
#
# This branch is detached from master history.
# It contains no dbs2go source tree.
# It contains no .github/workflows/.
# It contains only the passive evidence bundle and a README.

git checkout --orphan "$EVIDENCE_BRANCH" || exit 1

git rm -rf . >/dev/null 2>&1 || true

mkdir -p incident-evidence || exit 1

cp "$BUNDLE_TMP" "incident-evidence/$BUNDLE_NAME" || exit 1

cat > incident-evidence/README.md <<EOF
# Passive Megalodon CI incident evidence bundle

This orphan branch stores one passive Git bundle for the dbs2go Megalodon CI incident.

Bundle file:

    $BUNDLE_NAME

This branch is intentionally orphaned from master history.

This branch intentionally does not contain the dbs2go source tree.

This branch intentionally does not contain:

    .github/workflows/

Do not import this bundle into a working repository unless performing controlled forensic analysis.

The bundle preserves this incident range:

    $PRE_BAD..master

Incident commits preserved inside the bundle:

    BAD          $BAD
    REVERT       $REVERT
    MERGE_REVERT $MERGE_REVERT

No tag was created.
No branch points directly to the malicious commit chain.
EOF

git add incident-evidence/README.md "incident-evidence/$BUNDLE_NAME" || exit 1

git commit -m "incident: preserve passive Megalodon CI evidence bundle for ${SHORT}" || exit 1

# ----------------------------------------------------------------------
# 4. Clean local master directly
# ----------------------------------------------------------------------
#
# No cleanup branch is created.
# Local master is moved directly back to PRE_BAD.
#
# This removes from master history:
#
#   be9af07
#   73ac01f
#   a626518

git checkout master || exit 1

git reset --hard "$PRE_BAD" || exit 1

# ----------------------------------------------------------------------
# 5. Validate cleaned local master
# ----------------------------------------------------------------------

git log --graph --decorate --oneline --max-count=10

git status --short

git show --no-patch --pretty=fuller HEAD

git diff "$PRE_BAD" HEAD

# Expected:
# - HEAD is PRE_BAD
# - diff is empty
# - no upstream push has been performed yet

# ----------------------------------------------------------------------
# 6. Final local summary
# ----------------------------------------------------------------------

echo
echo "Orphan evidence branch:"
echo "  $EVIDENCE_BRANCH"

echo
echo "Passive evidence bundle stored on the orphan branch at:"
echo "  incident-evidence/$BUNDLE_NAME"

echo
echo "Cleaned local master HEAD:"
git rev-parse HEAD

echo
echo "No tags created."
echo "No cleanup branch created."
echo "No branch points directly to the malicious commit chain."
echo "No upstream push performed yet."
