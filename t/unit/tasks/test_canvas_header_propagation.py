"""Tests for explicit canvas header propagation.

Targeted run command:

    python -m pytest t/unit/tasks/test_canvas_header_propagation.py -q

Offline message fixtures live in ``t/unit/tasks/fixtures/``:

- ``canvas_header_propagation_input.json``: serialized nested canvas
  (chain -> group + chord + immutable leaf) whose root carries the
  business headers ``tenant`` and ``debug``.
- ``canvas_header_propagation_expected.json``: golden snapshot of the
  same canvas after ``propagate_headers(..., ['tenant'])``.
"""
import json
from pathlib import Path

import pytest

from celery.canvas import (
    HeaderPropagationPolicy,
    RESERVED_HEADER_KEYS,
    Signature,
    chain,
    chord,
    group,
    propagate_headers,
)

FIXTURES = Path(__file__).with_name('fixtures')
INPUT_FIXTURE = FIXTURES / 'canvas_header_propagation_input.json'
EXPECTED_FIXTURE = FIXTURES / 'canvas_header_propagation_expected.json'


def load_fixture(path):
    return json.loads(path.read_text(encoding='utf-8'))


def to_plain(node):
    """Recursively convert a signature tree to JSON-comparable data."""
    if isinstance(node, Signature):
        return {key: to_plain(value) for key, value in node.items()}
    if isinstance(node, dict):
        return {key: to_plain(value) for key, value in node.items()}
    if isinstance(node, (list, tuple)):
        return [to_plain(value) for value in node]
    return node


def build_canvas():
    # from_dict canonicalizes (and partly mutates) the input mapping,
    # so every build starts from a freshly loaded fixture.
    return Signature.from_dict(load_fixture(INPUT_FIXTURE))


def child_headers(sig):
    return sig.options.get('headers', {})


class test_header_propagation_policy:

    def test_merge_child_explicit_wins_and_filters_parent(self):
        policy = HeaderPropagationPolicy(['tenant'])
        merged = policy.merge(
            {'tenant': 'acme', 'debug': True}, {'tenant': 'child-a'})
        assert merged == {'tenant': 'child-a'}

    def test_merge_and_filter_never_mutate_inputs(self):
        policy = HeaderPropagationPolicy(['tenant'])
        inherited = {'tenant': 'acme', 'debug': True}
        explicit = {'tenant': 'child-a'}
        merged = policy.merge(inherited, explicit)
        assert inherited == {'tenant': 'acme', 'debug': True}
        assert explicit == {'tenant': 'child-a'}
        assert merged is not inherited and merged is not explicit
        filtered = policy.filter(inherited)
        assert filtered == {'tenant': 'acme'}
        assert filtered is not inherited

    def test_reserved_protocol_keys_rejected(self):
        for reserved in sorted(RESERVED_HEADER_KEYS):
            with pytest.raises(ValueError):
                HeaderPropagationPolicy(['tenant', reserved])

    def test_reserved_key_rejected_via_propagate_headers(self):
        sig = Signature('biz.t', options={'headers': {'tenant': 'acme'}})
        with pytest.raises(ValueError):
            propagate_headers(sig, ['tenant', 'root_id'])

    def test_policy_accepts_iterable_of_keys(self):
        policy = HeaderPropagationPolicy(iter(['tenant']))
        assert policy.allowed_keys == frozenset({'tenant'})
        assert 'tenant' in repr(policy)


class test_canvas_header_propagation:

    def setup_method(self):
        self.policy = HeaderPropagationPolicy(['tenant'])

    def test_only_allowed_keys_reach_nested_children(self):
        canvas = build_canvas()
        result = propagate_headers(canvas, self.policy)

        # the parent keeps its own headers exactly as they were
        assert result.options['headers'] == {'tenant': 'acme', 'debug': True}

        grp, chd, leaf = result.tasks
        # group child inherits only the allow-listed key
        assert child_headers(grp) == {'tenant': 'acme'}
        # nested group members: explicit tenant wins, debug never arrives
        assert child_headers(grp.tasks[0]) == {'tenant': 'child-a'}
        assert child_headers(grp.tasks[1]) == {'tenant': 'acme'}

        # chord child: header tasks and the body
        assert child_headers(chd) == {'tenant': 'acme'}
        header = chd.kwargs['header']
        assert isinstance(header, list) and len(header) == 2
        assert child_headers(header[0]) == {'tenant': 'acme'}
        assert child_headers(header[1]) == {'tenant': 'acme'}
        assert child_headers(chd.kwargs['body']) == {'tenant': 'body-tenant'}

        # immutable leaf still receives headers (they are options,
        # not parameters)
        assert child_headers(leaf) == {'tenant': 'acme'}

        # no child anywhere received the non-allowed ``debug`` key
        plain = to_plain(result)
        assert 'debug' not in json.dumps(
            [task['options'].get('headers', {}) for task in
             plain['kwargs']['tasks']])

    def test_matches_expected_fixture(self):
        result = propagate_headers(build_canvas(), self.policy)
        assert to_plain(result) == load_fixture(EXPECTED_FIXTURE)

    def test_source_signature_and_headers_not_mutated(self):
        canvas = build_canvas()
        root_headers = canvas.options['headers']
        snapshot = to_plain(canvas)

        propagate_headers(canvas, self.policy)

        assert to_plain(canvas) == snapshot
        assert canvas.options['headers'] is root_headers
        assert root_headers == {'tenant': 'acme', 'debug': True}

    def test_immutable_keeps_parameter_semantics(self):
        canvas = build_canvas()
        result = propagate_headers(canvas, self.policy)
        leaf = result.tasks[2]
        original_leaf = canvas.tasks[2]
        assert leaf.immutable is True
        assert list(leaf.args) == list(original_leaf.args) == [9]
        assert leaf.kwargs == original_leaf.kwargs == {}
        assert child_headers(leaf) == {'tenant': 'acme'}

    def test_child_protocol_fields_preserved_parent_reserved_dropped(self):
        child = Signature('biz.t', options={'headers': {'id': 'child-id'}})
        parent = chain(
            child, headers={'tenant': 'acme', 'root_id': 'root-1'})
        result = propagate_headers(parent, ['tenant'])

        # the parent's own copy keeps its headers untouched
        assert result.options['headers'] == {
            'tenant': 'acme', 'root_id': 'root-1'}
        propagated = result.tasks[0]
        # the child's own protocol field survives; the parent's reserved
        # ``root_id`` is not propagated
        assert child_headers(propagated) == {
            'tenant': 'acme', 'id': 'child-id'}
        # source child not modified in place
        assert child_headers(child) == {'id': 'child-id'}

    def test_signature_method_and_key_iterable_entry_points(self):
        canvas = build_canvas()
        via_method = canvas.propagate_headers(['tenant'])
        via_function = propagate_headers(canvas, ('tenant',))
        assert to_plain(via_method) == to_plain(via_function)
        assert child_headers(via_method.tasks[0]) == {'tenant': 'acme'}

    def test_group_and_chord_built_in_process(self):
        # same rules apply to canvases built without fixtures
        c = chain(
            group(
                Signature('biz.a', options={'headers': {'tenant': 'x'}}),
                Signature('biz.b'),
            ),
            chord(
                [Signature('biz.c'), Signature('biz.d')],
                body=Signature('biz.e'),
            ),
            headers={'tenant': 'acme', 'debug': True},
        )
        result = c.propagate_headers(HeaderPropagationPolicy(['tenant']))
        grp, chd = result.tasks
        assert child_headers(grp.tasks[0]) == {'tenant': 'x'}
        assert child_headers(grp.tasks[1]) == {'tenant': 'acme'}
        assert child_headers(chd.kwargs['header'][0]) == {
            'tenant': 'acme'}
        assert child_headers(chd.kwargs['header'][1]) == {
            'tenant': 'acme'}
        assert child_headers(chd.kwargs['body']) == {'tenant': 'acme'}
