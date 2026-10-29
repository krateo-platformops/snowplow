"""The bench's ONE rendering of an authn user: its kubernetes.io/basic-auth
password Secret and its users.basic.authn.krateo.io CR. STDLIB ONLY.

Two renderings used to exist, and they drifted. The data step's users
(lifecycle.create_synthetic_users) set spec.avatarURL; the fresh identity
classes (fresh_classes.manifest, #935) did not. The User CRD requires it
(authn-crds 0.27.3, basic.authn.krateo.io_users.yaml, v1alpha1:
spec.required = [avatarURL, displayName, passwordRef]), so run 17 attempt 4
had all 670 fresh Users refused (`spec.avatarURL: Required value`). Both now
come from here, and tests/test_fresh_classes_apply.py validates what this module
renders against that CRD's schema.
"""

from __future__ import annotations

API_VERSION = "basic.authn.krateo.io/v1alpha1"
# the avatars authn's UI shows (70 distinct pictures, as the data step's users always had)
AVATARS = 70


def avatar_url(i: int) -> str:
    return f"https://i.pravatar.cc/256?img={(i % AVATARS) + 1}"


def password_secret(name: str, namespace: str, password: str, labels: dict) -> dict:
    return {"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/basic-auth",
            "metadata": {"name": f"{name}-password", "namespace": namespace, "labels": dict(labels)},
            "stringData": {"password": password}}


def user(name: str, namespace: str, *, display_name: str, groups: list[str], avatar: str,
         labels: dict) -> dict:
    return {"apiVersion": API_VERSION, "kind": "User",
            "metadata": {"name": name, "namespace": namespace, "labels": dict(labels)},
            "spec": {"displayName": display_name, "avatarURL": avatar, "groups": list(groups),
                     "passwordRef": {"namespace": namespace, "name": f"{name}-password", "key": "password"}}}
