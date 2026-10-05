# Header Propagation Table

This document explains the explicit header propagation strategy for Canvas
publishing in Celery.

## Overview

When a parent signature publishes a child signature, only explicitly allowed
header keys propagate from parent to child. The child's own explicit values
take priority over propagated values. Celery internal reserved keys are never
overridden by business propagation.

## Configuration

Set the allowlist via the `task_header_propagation_allowlist` configuration:

```python
app.conf.task_header_propagation_allowlist = ['tenant', 'region']
```

If not configured, the default is an empty allowlist (nothing propagates).

## Propagation Rules

| Canvas Primitive | Parent -> Child Relationship | Propagation Direction |
|-----------------|------------------------------|----------------------|
| chain           | Previous task -> Next task   | Forward (task N -> task N+1) |
| group           | Group -> Each task           | Parent to all children |
| chord           | Chord -> Header (group)      | Parent to header |
| chord           | Chord -> Body                | Parent to body |

## Reserved Protocol Keys

The following keys are reserved by Celery and are never overridden by
business propagation:

| Key             | Description                          |
|-----------------|--------------------------------------|
| task_id         | Unique task identifier               |
| group_id        | Group identifier                     |
| chord           | Chord callback reference             |
| root_id         | Root task identifier                 |
| parent_id       | Parent task identifier               |
| group_index     | Index within a group                 |
| reply_to        | Reply queue for results              |
| chain           | Chain callback reference             |
| link            | Link callback reference              |
| link_error      | Error callback reference             |
| stamped_headers | Stamped header names                 |

## Propagation Behavior

### What Propagates

- Only keys in the allowlist propagate
- Values are copied (not referenced) to prevent in-place modification

### What Does Not Propagate

- Keys not in the allowlist
- Reserved protocol keys (even if in the allowlist)
- Keys already present in the child's headers (child priority)

### Child Priority

If a child signature has an explicit value for a header key, that value
takes priority over the propagated value:

```
Parent headers: {tenant: 'parent', debug: true}
Child headers:  {tenant: 'child'}
Allowlist:      ['tenant']
Result:         {tenant: 'child'}  # child value wins
```

## In-Place Modification Prevention

- Source signatures are cloned before propagation
- Parent headers are never modified
- Child headers are merged into a new dict, not modified in-place

## Immutable Signatures

Immutable signatures do not change their args/kwargs semantics. Header
propagation does not affect the immutability of a signature:

```python
s2 = add.s(3, 4).set(headers={}, immutable=True)
# s2 will not receive args from the previous task in the chain
```

## Examples

### Chain Propagation

```python
app.conf.task_header_propagation_allowlist = ['tenant']

s1 = add.s(1, 2).set(headers={'tenant': 'acme', 'debug': True})
s2 = add.s(3).set(headers={})
s3 = add.s(4).set(headers={})

chain(s1, s2, s3).apply()
# s2 receives: {tenant: 'acme'}  (debug is filtered out)
# s3 receives: {tenant: 'acme'}  (debug is filtered out)
```

### Group Propagation

```python
app.conf.task_header_propagation_allowlist = ['tenant']

g1 = add.s(1).set(headers={})
g2 = add.s(2).set(headers={})

group(g1, g2, headers={'tenant': 'acme', 'debug': True}).apply()
# g1 receives: {tenant: 'acme'}  (debug is filtered out)
# g2 receives: {tenant: 'acme'}  (debug is filtered out)
```

### Chord Propagation

```python
app.conf.task_header_propagation_allowlist = ['tenant']

c1 = add.s(1).set(headers={})
c2 = add.s(2).set(headers={})
body = body_task.s().set(headers={})

chord([c1, c2], body, headers={'tenant': 'acme', 'debug': True}).apply()
# c1 receives:   {tenant: 'acme'}  (debug is filtered out)
# c2 receives:   {tenant: 'acme'}  (debug is filtered out)
# body receives: {tenant: 'acme'}  (debug is filtered out)
```

## Testing

Run the targeted tests with:

```bash
pytest t/unit/test_header_propagation.py -v
```

Run specific test classes:

```bash
pytest t/unit/test_header_propagation.py::test_HeaderPropagation -v
pytest t/unit/test_header_propagation.py::test_ChainHeaderPropagation -v
pytest t/unit/test_header_propagation.py::test_GroupHeaderPropagation -v
pytest t/unit/test_header_propagation.py::test_ChordHeaderPropagation -v
```
