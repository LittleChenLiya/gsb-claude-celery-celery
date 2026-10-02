"""Route selection explanation demo.

Demonstrates :meth:`celery.app.routes.Router.explain`, a read-only
introspection entry point that reports which rule matched a task and the
final queue, without modifying the route cache or publishing a task.

Run directly::

    python examples/route_explain.py

The script adds its own worktree root to ``sys.path`` so it runs against
this checkout's celery rather than any globally installed copy.

The script prepares wildcard, exact, and default routes, then for each
task name prints the explanation and checks it against the actual
:meth:`~celery.app.routes.Router.route` result.
"""
import os
import sys

# Ensure this checkout's celery is imported, not a globally installed copy.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import Celery  # noqa: E402
from celery.app import routes  # noqa: E402


def build_app():
    app = Celery('route_explain_demo')
    app.conf.task_default_queue = 'default'
    app.conf.task_default_exchange = 'default'
    app.conf.task_default_exchange_type = 'direct'
    app.conf.task_default_routing_key = 'default'
    app.conf.task_queues = {
        'default': {
            'exchange': 'default',
            'exchange_type': 'direct',
            'routing_key': 'default',
        },
        'exact_q': {
            'exchange': 'exact_ex',
            'exchange_type': 'direct',
            'routing_key': 'exact_rk',
        },
        'wild_q': {
            'exchange': 'wild_ex',
            'exchange_type': 'topic',
            'routing_key': 'wild.#',
        },
    }
    return app


def build_router(app):
    task_routes = [
        # wildcard rule
        {'proj.tasks.wild*': {'queue': 'wild_q'}},
        # exact rule
        {'proj.tasks.exact': {'queue': 'exact_q'}},
    ]
    return routes.Router(
        routes.prepare(task_routes),
        app.amqp.queues,
        app=app,
    )


def main():
    app = build_app()
    router = build_router(app)

    default_name = app.conf.task_default_queue
    cases = [
        ('proj.tasks.wild', 'wild_q'),        # wildcard match
        ('proj.tasks.wild.card', 'wild_q'),   # wildcard match
        ('proj.tasks.exact', 'exact_q'),      # exact match
        ('proj.tasks.other', default_name),   # default (unmatched)
        ('celery.unrelated', default_name),   # default (unmatched)
    ]

    print(f"{'task':<28}{'explanation':<14}{'queue':<12}{'actual':<12}match")
    print('-' * 78)
    all_ok = True
    for name, expected in cases:
        info = router.explain(name)
        actual = router.route({}, name)
        actual_name = actual['queue'].name
        ok = (info['queue'] == expected == actual_name)
        all_ok = all_ok and ok
        kind = 'matched' if info['matched'] else 'default'
        print(
            f"{name:<28}{kind:<14}{info['queue']!s:<12}"
            f"{actual_name!s:<12}{ok}"
        )
        if info['matched']:
            print(f"    rule: {info['rule']!r}  route: {info['route']}")
        else:
            print(f"    default_source: {info['default_source']}")

    print('-' * 78)
    print('ALL MATCH' if all_ok else 'MISMATCH')
    return 0 if all_ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
