"""Tests for explicit header propagation strategy in Canvas publishing.

These tests verify that:
- Only permitted keys propagate from parent to child signatures
- Childs explicit values take priority over propagated values
- chain, group, and chord propagate according to real child relationships
- immutable does not change parameter semantics
- Source headers and Signatures cannot be modified in-place
- Celery internal reserved keys are preserved; business propagation cannot
  override protocol fields
"""
import pytest

from celery import Celery, chain, chord, group
from celery.canvas import HeaderPropagation, get_header_propagation


class test_HeaderPropagation:
    """Unit tests for the HeaderPropagation strategy class."""

    def test_only_allowed_keys_propagate(self):
        p = HeaderPropagation(allowed_keys=["tenant"])
        parent_opts = {"headers": {"tenant": "acme", "debug": True}}
        child_opts = {"headers": {}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {"tenant": "acme"}

    def test_empty_allowlist_propagates_nothing(self):
        p = HeaderPropagation()
        parent_opts = {"headers": {"tenant": "acme", "debug": True}}
        child_opts = {"headers": {}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {}

    def test_child_explicit_value_takes_priority(self):
        p = HeaderPropagation(allowed_keys=["tenant"])
        parent_opts = {"headers": {"tenant": "parent"}}
        child_opts = {"headers": {"tenant": "child"}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {"tenant": "child"}

    def test_reserved_keys_are_never_overridden(self):
        p = HeaderPropagation(allowed_keys=[
            "tenant", "task_id", "group_id", "chord", "root_id",
            "parent_id", "group_index", "reply_to", "chain", "link",
            "link_error", "stamped_headers",
        ])
        parent_opts = {"headers": {
            "tenant": "acme",
            "task_id": "parent-task",
            "group_id": "parent-group",
            "chord": "parent-chord",
            "root_id": "parent-root",
            "parent_id": "parent-parent",
            "group_index": 0,
            "reply_to": "parent-reply",
            "chain": "parent-chain",
            "link": "parent-link",
            "link_error": "parent-link-error",
            "stamped_headers": ["tenant"],
        }}
        child_opts = {"headers": {}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {"tenant": "acme"}

    def test_source_not_modified_in_place(self):
        p = HeaderPropagation(allowed_keys=["tenant"])
        parent_opts = {"headers": {"tenant": "acme", "debug": True}}
        child_opts = {"headers": {}}
        parent_before = dict(parent_opts)
        child_before = dict(child_opts)
        p.apply(parent_opts, child_opts)
        assert parent_opts == parent_before
        assert child_opts == child_before

    def test_propagate_returns_only_new_headers(self):
        p = HeaderPropagation(allowed_keys=["tenant"])
        parent_opts = {"headers": {"tenant": "acme", "debug": True}}
        child_opts = {"headers": {"existing": "value"}}
        propagated = p.propagate(parent_opts, child_opts)
        assert propagated == {"tenant": "acme"}

    def test_apply_merges_with_child_headers(self):
        p = HeaderPropagation(allowed_keys=["tenant"])
        parent_opts = {"headers": {"tenant": "acme", "debug": True}}
        child_opts = {"headers": {"existing": "value"}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {"existing": "value", "tenant": "acme"}

    def test_custom_reserved_keys(self):
        p = HeaderPropagation(
            allowed_keys=["tenant", "custom_reserved"],
            reserved_keys=["custom_reserved"],
        )
        parent_opts = {"headers": {"tenant": "acme", "custom_reserved": "blocked"}}
        child_opts = {"headers": {}}
        result = p.apply(parent_opts, child_opts)
        assert result["headers"] == {"tenant": "acme"}

    def test_get_header_propagation_reads_config(self):
        app = Celery("test")
        app.conf.task_header_propagation_allowlist = ["tenant", "region"]
        p = get_header_propagation(app)
        assert p.allowed_keys == {"tenant", "region"}

    def test_get_header_propagation_default_empty(self):
        app = Celery("test")
        app.conf.task_header_propagation_allowlist = None
        p = get_header_propagation(app)
        assert p.allowed_keys == set()


class test_ChainHeaderPropagation:
    """Integration tests for header propagation in chain."""

    def test_chain_propagates_allowed_headers(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        received = []

        @app.task(bind=True)
        def add(self, x, y):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return x + y

        s1 = add.s(1, 2).set(headers={"tenant": "acme", "debug": True})
        s2 = add.s(3).set(headers={})
        c = chain(s1, s2)
        c.apply()

        assert received[0] == {"tenant": "acme", "debug": True}
        assert received[1] == {"tenant": "acme"}

    def test_chain_nested_children_get_tenant_only(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        received = []

        @app.task(bind=True)
        def add(self, x, y):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return x + y

        s1 = add.s(1, 2).set(headers={"tenant": "acme", "debug": True})
        s2 = add.s(3).set(headers={})
        s3 = add.s(4).set(headers={})
        chain(s1, s2, s3).apply()

        assert received[1] == {"tenant": "acme"}
        assert received[2] == {"tenant": "acme"}

    def test_chain_child_tenant_takes_priority(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        received = []

        @app.task(bind=True)
        def add(self, x, y):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return x + y

        s1 = add.s(1, 2).set(headers={"tenant": "parent"})
        s2 = add.s(3).set(headers={"tenant": "child"})
        chain(s1, s2).apply()

        assert received[1] == {"tenant": "child"}

    def test_chain_source_not_modified(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        @app.task
        def add(x, y):
            return x + y

        s1 = add.s(1, 2).set(headers={"tenant": "acme", "debug": True})
        s2 = add.s(3).set(headers={})
        s1_before = dict(s1.options)
        s2_before = dict(s2.options)
        chain(s1, s2).apply()

        assert s1.options == s1_before
        assert s2.options == s2_before

    def test_chain_immutable_semantics_preserved(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        @app.task
        def add(x, y):
            return x + y

        s1 = add.s(1, 2).set(headers={"tenant": "acme"})
        s2 = add.s(3, 4).set(headers={}, immutable=True)
        result = chain(s1, s2).apply()

        assert result.get() == 7


class test_GroupHeaderPropagation:
    """Integration tests for header propagation in group."""

    def test_group_propagates_allowed_headers(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        received = []

        @app.task(bind=True)
        def add(self, x, y):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return x + y

        g1 = add.s(1, 2).set(headers={})
        g2 = add.s(3, 4).set(headers={})
        group(g1, g2, headers={"tenant": "acme", "debug": True}).apply()

        assert received[0] == {"tenant": "acme"}
        assert received[1] == {"tenant": "acme"}

    def test_group_source_not_modified(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        @app.task
        def add(x, y):
            return x + y

        g1 = add.s(1, 2).set(headers={})
        g2 = add.s(3, 4).set(headers={})
        g1_before = dict(g1.options)
        g2_before = dict(g2.options)
        group(g1, g2, headers={"tenant": "acme"}).apply()

        assert g1.options == g1_before
        assert g2.options == g2_before


class test_ChordHeaderPropagation:
    """Integration tests for header propagation in chord."""

    def test_chord_propagates_allowed_headers(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        received = []

        @app.task(bind=True)
        def add(self, x, y):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return x + y

        @app.task(bind=True)
        def body_task(self, results):
            received.append(dict(self.request.headers) if self.request.headers else {})
            return sum(results)

        c1 = add.s(1, 2).set(headers={})
        c2 = add.s(3, 4).set(headers={})
        body = body_task.s().set(headers={})
        chord([c1, c2], body, headers={"tenant": "acme", "debug": True}).apply()

        assert received[0] == {"tenant": "acme"}
        assert received[1] == {"tenant": "acme"}
        assert received[2] == {"tenant": "acme"}

    def test_chord_source_not_modified(self):
        app = Celery("test")
        app.conf.task_always_eager = True
        app.conf.task_header_propagation_allowlist = ["tenant"]

        @app.task
        def add(x, y):
            return x + y

        @app.task
        def body_task(results):
            return sum(results)

        c1 = add.s(1, 2).set(headers={})
        c2 = add.s(3, 4).set(headers={})
        body = body_task.s().set(headers={})
        c1_before = dict(c1.options)
        c2_before = dict(c2.options)
        body_before = dict(body.options)
        chord([c1, c2], body, headers={"tenant": "acme"}).apply()

        assert c1.options == c1_before
        assert c2.options == c2_before
        assert body.options == body_before
