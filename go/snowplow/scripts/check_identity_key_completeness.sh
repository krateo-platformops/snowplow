#!/bin/bash
# check_identity_key_completeness.sh — #449 identity-key completeness CI guard.
#
# THE INVARIANT: a cache or reuse key must fold EVERY identity input that
# determines the cached content, or the store must be declared identity-free
# and re-gated per requester at serve. It broke three times on three code paths
# (#423 first-match binding only; #432 the SeedResolveMemo without the RBAC
# class; #435 the raFullList key without the sub-generation) because each mint
# site assembled the identity dimensions by hand.
#
# The AST checker in scripts/checkidentitykeys enforces the single-source
# structure #449 introduced (rules R0-R5, see its package doc):
#
#   - rbac.IdentityClassOf is the only class derivation;
#   - ResolvedKeyInputs.SetIdentity is the only writer of the identity fields;
#   - every identity-bound ComputeKey site goes through SetIdentity, and no
#     identity-free class (cache.identityFreeClasses) ever does;
#   - the SeedResolveMemo key folds the class;
#   - every memo / cache / store map is declared in the identity registry.
#
# The dimension set is read from ComputeKey itself, so a new dimension is
# required at every site at once. Every build tag is covered (files are parsed
# directly; build constraints are not applied). Test files and testdata/ are
# skipped. The semantic half — the class is sufficient and rotates — is the
# dispatchers property test identity_class_property_test.go.
#
# Run it locally before pushing:
#
#   bash scripts/check_identity_key_completeness.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

go run ./scripts/checkidentitykeys .
