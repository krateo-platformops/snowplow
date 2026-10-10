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
# structure #449 introduced, in TWO CATEGORIES (rules R0-R5 and S0/S2/S3, see
# its package doc):
#
#   - rbac.IdentityClassOf is the only class derivation;
#   - ResolvedKeyInputs.SetIdentity is the only writer of the identity fields;
#   - every identity-bound ComputeKey site goes through SetIdentity, and no
#     identity-free class (cache.identityFreeClasses) ever does;
#   - the SeedResolveMemo key folds the class;
#   - every memo / cache / store map is declared in the identity registry.
#
# THE SECOND CATEGORY (#180). An identity dimension is a function of the
# REQUESTER ALONE — which is why rbac.IdentityClassOf takes only a UserInfo and
# why one derivation suffices. A UAF-scope digest is a function of (requester,
# ACCESS DOMAIN), and the domain comes from the CR/apiRef chain, so it cannot be
# produced by IdentityClassOf at all. It therefore gets its own branch in
# ComputeKey, its own single writer (SetScope) and its own single derivation
# (ScopeClassOf), and S0/S2/S3 are the exact analogues of R0/R2/R3:
#
#   - ScopeClassOf is the only scope derivation, and it must take BOTH inputs —
#     a one-argument derivation cannot see the domain and can only fabricate a
#     scope, which is the mistake the category exists to prevent;
#   - ResolvedKeyInputs.SetScope is the only writer of the scope fields;
#   - the identity and scope branches must stay DISJOINT, or the scope dimension
#     is collected as an identity one and fails R0 for a reason that looks like
#     a missing writer.
#
# The S rules are INERT until a scope branch exists, so they cannot fire on a
# tree that has none.
#
# The dimension set of EACH category is read from ComputeKey itself, so a new
# dimension is required at every site at once. Every build tag is covered (files are parsed
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
