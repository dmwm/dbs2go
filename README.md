# Passive Megalodon CI incident evidence bundle

This orphan branch stores one passive Git bundle for the dbs2go Megalodon CI incident.

Bundle file:

    dbs2go-megalodon-ci-incident-chain-be9af07.bundle

This branch is intentionally orphaned from master history.

This branch intentionally does not contain the dbs2go source tree.

This branch intentionally does not contain:

    .github/workflows/

Do not import this bundle into a working repository unless performing controlled forensic analysis.

The bundle preserves this incident range:

    964e2e45e510af2852d1e5ff4e89018a991490d5..master

Incident commits preserved inside the bundle:

    BAD          be9af07e64f12518455308cfde7749bfa6c5fd1d
    REVERT       73ac01f971eb143ec2226ee9a7b07a75534b4e85
    MERGE_REVERT a626518253526586b23933e7f3f61e5fbcb2866c

No tag was created.
No branch points directly to the malicious commit chain.


## Forensic use of the evidence bundle

This branch contains a passive Git bundle with the incident history.

The bundle is not an active branch or tag. It cannot be checked out directly unless it is deliberately imported into a separate Git repository.

Bundle file:

    incident-evidence/dbs2go-megalodon-ci-incident-chain-be9af07.bundle

### Safety rules

Use a separate throwaway analysis environment.

Do not import the bundle into a normal development clone.

Do not run GitHub Actions locally.

Do not execute any workflow, shell fragment, decoded payload, or script recovered from the bundle.

Do not run `act` or any local CI runner.

Do not use an environment containing GitHub tokens, cloud credentials, SSH keys, registry credentials, or mounted home directories.

Avoid outbound network access unless the environment is intentionally configured for malware-analysis work.

### Import the bundle for static analysis

From outside the repository, create a separate analysis clone:

    git clone --no-checkout path/to/dbs2go-megalodon-ci-incident-chain-be9af07.bundle dbs2go-bundle-analysis
    cd dbs2go-bundle-analysis
    git log --graph --decorate --oneline --all

The `--no-checkout` option is intentional. It imports the Git history without populating the working tree with the malicious workflow file.

### Inspect the malicious workflow without checking out the tree

    BAD=be9af07e64f12518455308cfde7749bfa6c5fd1d
    PRE_BAD=964e2e45e510af2852d1e5ff4e89018a991490d5

    git show "$BAD":.github/workflows/build.yml > malicious-build-workflow.yml.txt

    git diff "$PRE_BAD" "$BAD" -- .github/workflows/build.yml > malicious-workflow.diff

    git show --name-status --oneline "$BAD" > malicious-commit-files.txt

These commands extract the malicious workflow as inert text files for static inspection.

### Optional full checkout in an isolated lab

A full checkout may be useful when analysis tools require a real working tree or when the exact repository layout at the malicious commit is needed.

Only do this in an isolated throwaway environment with no credentials and no trusted mounts:

    git checkout --detach be9af07e64f12518455308cfde7749bfa6c5fd1d

After this command, the working tree contains the repository state from the malicious commit, including the malicious workflow file.

Do not execute anything from the checkout.

### Safer alternatives to full checkout

Prefer object-level Git inspection whenever possible:

    git show $BAD:path/to/file
    git diff $PRE_BAD $BAD
    git ls-tree -r $BAD
    git grep <PATTERN> $BAD

Concrete examples:

    git show be9af07e64f12518455308cfde7749bfa6c5fd1d:.github/workflows/build.yml

    git diff 964e2e45e510af2852d1e5ff4e89018a991490d5 be9af07e64f12518455308cfde7749bfa6c5fd1d -- .github/workflows/build.yml

    git ls-tree -r be9af07e64f12518455308cfde7749bfa6c5fd1d

### Incident commits preserved in the bundle

    PRE_BAD      964e2e45e510af2852d1e5ff4e89018a991490d5
    BAD          be9af07e64f12518455308cfde7749bfa6c5fd1d
    REVERT       73ac01f971eb143ec2226ee9a7b07a75534b4e85
    MERGE_REVERT a626518253526586b23933e7f3f61e5fbcb2866c

The cleaned `master` branch should not contain the `BAD`, `REVERT`, or `MERGE_REVERT` commits in its history.

