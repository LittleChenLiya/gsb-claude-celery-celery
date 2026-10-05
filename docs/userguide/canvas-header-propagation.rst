.. _canvas-header-propagation:

============================
 Canvas Header Propagation
============================

.. currentmodule:: celery.canvas

When a canvas is published, business headers (for example a ``tenant``
or ``request_id`` stored in ``options['headers']``) often need to reach
every real child signature.  :func:`propagate_headers` does this
explicitly: only the keys named in a
:class:`HeaderPropagationPolicy` may flow from a parent signature into
its children, and a child's own explicit values always win.

.. code-block:: python

    from celery.canvas import HeaderPropagationPolicy, propagate_headers

    policy = HeaderPropagationPolicy(['tenant'])
    published = propagate_headers(canvas, policy)   # or canvas.propagate_headers(policy)

``propagate_headers`` returns a **copy**: neither the source signature
tree nor any of its ``options['headers']`` mappings is modified in
place.

Propagation table
-----------------

Headers are read from ``options['headers']`` of each signature.  The
*effective* headers of a node are ``policy.merge(inherited, own)`` --
the allow-listed inherited keys overlaid by the node's own explicit
headers.  Containers forward ``policy.filter(effective)`` to their
children, recursively.

============= ============================ ===================================
Primitive     Real children                Propagation rule
============= ============================ ===================================
``chain``     every task in ``tasks``      each task inherits the chain's
                                           allow-listed effective headers
``group``     every member in ``tasks``    each member inherits the group's
                                           allow-listed effective headers
``chord``     every header task **and**    header tasks and body all inherit
              the ``body``                 the chord's allow-listed
                                           effective headers
leaf task     --                           merged headers are set on the
                                           copied signature's options
============= ============================ ===================================

Precedence and filtering rules:

1. Only policy-allowed keys leave a parent; everything else
   (e.g. ``debug``) stays on the parent and never reaches children.
2. A child's explicit ``options['headers']`` value for a key always
   overrides the inherited value for that key (``tenant: child-a``
   beats ``tenant: acme``).
3. Nested primitives are descended recursively, so grandchildren
   inherit from their immediate parent's effective headers.
4. ``immutable`` signatures are treated like any other leaf: header
   propagation only touches ``options['headers']`` and never modifies
   ``args``/``kwargs``, so immutability keeps its exact parameter
   semantics.

Reserved protocol fields
------------------------

Celery's wire-protocol and runtime bookkeeping header keys
(:data:`RESERVED_HEADER_KEYS`: ``id``, ``task``, ``root_id``,
``parent_id``, ``group``, ``group_index``, ``chord``, ``callbacks``,
``errbacks``, ``chain``, ``eta``, ``expires``, ``retries``,
``timelimit``, ``stamped_headers``, ``stamps``, ...) are owned by the
framework:

* Allow-listing any of them raises :exc:`ValueError`, so business
  propagation can never override protocol fields.
* Reserved keys found in a *parent's* headers are simply not
  propagated.
* Reserved keys a *child* set explicitly are preserved untouched.

Example
-------

Given a chain whose root carries ``{'tenant': 'acme', 'debug': True}``
and the policy ``['tenant']``:

* nested group/chord/leaf children receive ``{'tenant': 'acme'}``;
* a child that declared ``{'tenant': 'child-a'}`` keeps ``child-a``;
* ``debug`` appears on no child.

Offline fixtures and targeted tests
-----------------------------------

Serialized before/after canvas messages live in
``t/unit/tasks/fixtures/canvas_header_propagation_input.json`` and
``canvas_header_propagation_expected.json``.  Run the focused test
module with:

.. code-block:: console

    $ python -m pytest t/unit/tasks/test_canvas_header_propagation.py -q
