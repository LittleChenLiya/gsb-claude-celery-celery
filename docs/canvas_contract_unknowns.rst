========================================
Canvas Contract Checker: Unknown Items
========================================

This document describes the items that the offline canvas contract checker
marks as **unknown** and explains why they cannot be statically determined.

The checker follows a strict principle: **never pretend that validation
succeeded when the input could not be fully analyzed**.  When a value cannot
be statically determined, it is marked as ``unknown`` (or reported as an
``info``-severity issue) rather than being silently treated as valid.

.. contents::
   :local:
   :depth: 2

--------------------------------
1. Unknown upstream return type
--------------------------------

**Issue type:** ``unknown_injection``

**Field:** ``SignatureInfo.return_type`` (``"unknown"``)

**When it occurs:**

A task in a chain (or a chord body) receives the result of the previous
task as its first positional argument.  The checker cannot determine the
return type of the upstream task because:

* The task body (``run`` method) can return any value at runtime.
* Python functions do not enforce return type annotations.
* Even when a return annotation is present, the checker does not assume
  that the annotation is accurate.

**What the checker does:**

* Marks ``return_type`` as ``"unknown"``.
* Reports an ``info``-severity issue of type ``unknown_injection`` at the
  canvas path of the receiving task.
* **Does not** claim that the type validation passed.

**What the checker still validates:**

* The number of positional arguments (bound args + injected arg) against the
  parameter count of the receiving task.  This check is independent of the
  return type.

**Example:**

.. code-block:: python

   chain(add.s(1, 2), one.s())

The ``one`` task receives the result of ``add``.  The return type of ``add``
is marked as ``unknown``, and an ``unknown_injection`` info issue is reported
at ``chain[1]``.

-----------------------------------
2. Unknown parameter count
-----------------------------------

**Issue type:** ``unknown_param_count``

**Field:** ``SignatureInfo.param_count`` (``None``)

**When it occurs:**

The checker cannot determine the number of positional parameters that a task
accepts because:

* The task's ``run`` method is not accessible (e.g., ``__wrapped__`` and
  ``run`` are both ``None``).
* The task's signature cannot be inspected (e.g., it is a built-in or a
  C extension).
* The task is not registered in the app (in this case, an
  ``unregistered_task`` error is reported instead).

**What the checker does:**

* Marks ``param_count`` as ``None``.
* Reports an ``info``-severity issue of type ``unknown_param_count`` at the
  canvas path of the task.
* **Skips** the parameter count validation for this task.

**What the checker still validates:**

* Whether the task is registered (if not, an error is reported).
* The canvas path and the structure of the canvas.

**Example:**

.. code-block:: python

   class OpaqueTask:
       name = 'opaque_task'
       __wrapped__ = None
       run = None

   app._tasks['opaque_task'] = OpaqueTask()
   signature('opaque_task')

The parameter count of ``opaque_task`` cannot be determined, so it is marked
as ``unknown`` and the parameter count validation is skipped.

-----------------------------------
3. Unknown injection kind
-----------------------------------

**Field:** ``SignatureInfo.injection_kind``

**Possible values:** ``"none"``, ``"single"``, ``"list"``, ``"unknown"``

**When it occurs:**

The ``injection_kind`` field describes the form of the upstream result:

* ``"none"`` -- no upstream result is injected (e.g., the first task in a
  chain, or an immutable signature).
* ``"single"`` -- a single value is injected (e.g., the result of a single
  task in a chain).
* ``"list"`` -- a list of values is injected (e.g., the result of a group
  or a chord header).
* ``"unknown"`` -- the injection form cannot be determined.

**What the checker does:**

* In practice, the checker can always determine the injection kind from the
  canvas structure (chain, group, chord).  The ``"unknown"`` value is
  reserved for future use or for canvas types that cannot be analyzed.
* When the injection kind is ``"unknown"``, the checker does not pretend
  that the injection is valid.

-----------------------------------
4. Immutable signatures
-----------------------------------

**Issue type:** ``immutable_injection``

**When it occurs:**

A signature marked as immutable (``.si(...)``) is in a position where it
would normally receive the upstream result (e.g., as the second task in a
chain).

**What the checker does:**

* Reports an ``info``-severity issue of type ``immutable_injection`` at the
  canvas path of the immutable signature.
* Sets ``receives_injection`` to ``False`` for the immutable signature.
* **Does not** report a parameter count conflict for the missing injection.

**Rationale:**

An immutable signature explicitly opts out of receiving the upstream result.
This is intentional behavior, not an error.  The checker reports it as
``info`` so that the user is aware that the injection will not happen.

**Example:**

.. code-block:: python

   chain(add.s(1, 2), one.si(9))

The ``one.si(9)`` signature is immutable, so it will not receive the result
of ``add``.  An ``immutable_injection`` info issue is reported at
``chain[1]``.

-----------------------------------
5. Unregistered tasks
-----------------------------------

**Issue type:** ``unregistered_task``

**When it occurs:**

A signature references a task name that is not registered in the current
app's task registry.

**What the checker does:**

* Reports an ``error``-severity issue of type ``unregistered_task`` at the
  canvas path of the signature.
* **Skips** the parameter count validation for this task (since the task
  body is not available).

**Rationale:**

An unregistered task cannot be executed by the worker.  This is a definite
error that must be fixed before the canvas can be published.

**Example:**

.. code-block:: python

   chain(add.s(1, 2), signature('nonexistent_task', args=(1,)))

The task ``nonexistent_task`` is not registered in the app.  An
``unregistered_task`` error is reported at ``chain[1]``.

-----------------------------------
6. Parameter count conflicts
-----------------------------------

**Issue type:** ``param_count_conflict``

**When it occurs:**

A task accepts fewer positional arguments than the number of arguments that
would be passed at runtime:

* The number of bound positional arguments (``sig.args``).
* Plus one injected upstream result (if the task receives an injection).

**What the checker does:**

* Reports an ``error``-severity issue of type ``param_count_conflict`` at the
  canvas path of the task.
* The message includes the parameter count, the number of bound args, the
  number of injected args, and the total.

**What the checker does not do:**

* Does not validate keyword arguments (``sig.kwargs``) against the task's
  keyword parameters.  This is a potential future enhancement.
* Does not validate the types of the arguments.  This is impossible to do
  statically for dynamic Python values.

**Example:**

.. code-block:: python

   chain(add.s(1, 2), one.s(9))

The ``one`` task accepts 1 positional argument.  It has 1 bound arg (``9``)
and receives 1 injected arg (the result of ``add``), for a total of 2.  A
``param_count_conflict`` error is reported at ``chain[1]``.

-----------------------------------
Summary of issue types
-----------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Issue type
     - Severity
     - Description
   * - ``unregistered_task``
     - error
     - A task is not registered in the app.
   * - ``param_count_conflict``
     - error
     - A task accepts fewer positional args than would be passed.
   * - ``immutable_injection``
     - info
     - An immutable signature will not accept the upstream result.
   * - ``unknown_param_count``
     - info
     - The parameter count cannot be determined.
   * - ``unknown_injection``
     - info
     - The upstream return type cannot be determined.
   * - ``unknown_canvas_type``
     - error
     - The canvas type cannot be analyzed.

-----------------------------------
Design principles
-----------------------------------

1. **Offline only:** The checker reads only the task registry of the current
   app.  It does not connect to the broker, execute any task, or send any
   message.

2. **No mutation:** The checker never modifies the original
   :class:`~celery.canvas.Signature` objects.  It only reads their
   attributes.

3. **No pretense:** When a value cannot be statically determined, the checker
   marks it as ``unknown`` instead of pretending that validation succeeded.

4. **Path preservation:** Every finding is located by a canvas path such as
   ``chain[1].group[0]``, so the user can identify exactly where the issue
   is in the canvas.

5. **Deterministic checks:** The parameter count conflict and unregistered
   task checks are deterministic and do not depend on runtime values.
