"""Task Routing.

Contains utilities for working with task routers, (:setting:`task_routes`).
"""
import fnmatch
import re
from collections import OrderedDict
from collections.abc import Mapping

from kombu import Queue

from celery.exceptions import QueueNotFound
from celery.utils.collections import lpmerge
from celery.utils.functional import maybe_evaluate, mlazy
from celery.utils.imports import symbol_by_name

try:
    Pattern = re._pattern_type
except AttributeError:  # pragma: no cover
    # for support Python 3.7
    Pattern = re.Pattern

__all__ = ('MapRoute', 'Router', 'expand_router_string', 'prepare')


class MapRoute:
    """Creates a router out of a :class:`dict`."""

    def __init__(self, map):
        map = map.items() if isinstance(map, Mapping) else map
        self.map = {}
        self.patterns = OrderedDict()
        for k, v in map:
            if isinstance(k, Pattern):
                self.patterns[k] = v
            elif '*' in k:
                self.patterns[re.compile(fnmatch.translate(k))] = v
            else:
                self.map[k] = v

    def __call__(self, name, *args, **kwargs):
        try:
            return dict(self.map[name])
        except KeyError:
            pass
        except ValueError:
            return {'queue': self.map[name]}
        for regex, route in self.patterns.items():
            if regex.match(name):
                try:
                    return dict(route)
                except ValueError:
                    return {'queue': route}

    def explain(self, name):
        """Explain which rule in this map matches task ``name``.

        Read-only: returns a description of the matching rule without
        modifying the route map or expanding any destination.
        """
        if name in self.map:
            return {
                'matched': True,
                'match_type': 'exact',
                'key': name,
                'route': self(name),
            }
        for regex, route in self.patterns.items():
            if regex.match(name):
                return {
                    'matched': True,
                    'match_type': 'pattern',
                    'key': regex.pattern,
                    'route': self(name),
                }
        return {'matched': False, 'match_type': None, 'key': None,
                'route': None}


class Router:
    """Route tasks based on the :setting:`task_routes` setting."""

    def __init__(self, routes=None, queues=None,
                 create_missing=False, app=None):
        self.app = app
        self.queues = {} if queues is None else queues
        self.routes = [] if routes is None else routes
        self.create_missing = create_missing

    def route(self, options, name, args=(), kwargs=None, task_type=None):
        kwargs = {} if not kwargs else kwargs
        options = self.expand_destination(options)  # expands 'queue'
        if self.routes:
            route = self.lookup_route(name, args, kwargs, options, task_type)
            if route:  # expands 'queue' in route.
                return lpmerge(self.expand_destination(route), options)
        if 'queue' not in options:
            options = lpmerge(self.expand_destination(
                self.app.conf.task_default_queue), options)
        return options

    def explain(self, options, name, args=(), kwargs=None, task_type=None):
        """Explain how task ``name`` would be routed, without routing it.

        Read-only entry point: it reports which router and rule matched
        (and the resulting queue), but never mutates the route tables or
        the caller's ``options``, and never publishes anything.

        Returns a dict with the keys:

        - ``task``: name of the task explained.
        - ``matched``: True if a configured route matched.
        - ``router``/``router_index``: the matching router and its
          position in :setting:`task_routes` (None when unmatched).
        - ``rule``: the raw route returned by the matching router.
        - ``detail``: per-rule explanation from the matching router
          itself (e.g. :class:`MapRoute` reports the exact key or
          glob/regex pattern that matched), when available.
        - ``route``: the final merged route options (same result as
          :meth:`route` would produce).
        - ``queue``: name of the final queue (or None).
        - ``default_source``: when no route matched, where the queue
          comes from: ``'task_default_queue'`` (the setting, see
          ``default_value``) or ``'options'`` (queue passed in
          ``options``).  None when a route matched.
        """
        kwargs = {} if not kwargs else kwargs
        # work on copies so the caller's options and any cached route
        # dicts are never mutated (expand_destination pops 'queue').
        options = self.expand_destination(dict(options))
        explanation = {
            'task': name,
            'matched': False,
            'router': None,
            'router_index': None,
            'rule': None,
            'detail': None,
            'route': None,
            'queue': None,
            'default_source': None,
            'default_value': None,
        }
        route = None
        if self.routes:
            # same traversal as lookup_route(): first non-None wins.
            for index, router in enumerate(self.routes):
                route = self.query_router(
                    router, name, args, kwargs, options, task_type)
                if route is not None:
                    if route:  # mirrors the truthiness check in route()
                        evaluated = maybe_evaluate(router)
                        detail = getattr(evaluated, 'explain', None)
                        explanation.update(
                            matched=True,
                            router=evaluated,
                            router_index=index,
                            rule=route,
                            detail=(detail(name)
                                    if callable(detail) else None),
                        )
                    break
        if explanation['matched']:
            if isinstance(route, Mapping):
                route = dict(route)  # don't mutate cached route
            # expands 'queue' in route.
            options = lpmerge(self.expand_destination(route), options)
        elif 'queue' not in options:
            default = self.app.conf.task_default_queue
            explanation['default_source'] = 'task_default_queue'
            explanation['default_value'] = default
            options = lpmerge(self.expand_destination(default), options)
        else:
            explanation['default_source'] = 'options'
        explanation['route'] = options
        queue = options.get('queue')
        explanation['queue'] = getattr(queue, 'name', queue)
        return explanation

    def expand_destination(self, route):
        # Route can be a queue name: convenient for direct exchanges.
        if isinstance(route, str):
            queue, route = route, {}
        else:
            # can use defaults from configured queue, but override specific
            # things (like the routing_key): great for topic exchanges.
            queue = route.pop('queue', None)

        if queue:
            if isinstance(queue, Queue):
                route['queue'] = queue
            else:
                try:
                    route['queue'] = self.queues[queue]
                except KeyError:
                    raise QueueNotFound(
                        f'Queue {queue!r} missing from task_queues')
        return route

    def lookup_route(self, name,
                     args=None, kwargs=None, options=None, task_type=None):
        query = self.query_router
        for router in self.routes:
            route = query(router, name, args, kwargs, options, task_type)
            if route is not None:
                return route

    def query_router(self, router, task, args, kwargs, options, task_type):
        router = maybe_evaluate(router)
        if hasattr(router, 'route_for_task'):
            # pre 4.0 router class
            return router.route_for_task(task, args, kwargs)
        return router(task, args, kwargs, options, task=task_type)


def expand_router_string(router):
    router = symbol_by_name(router)
    if hasattr(router, 'route_for_task'):
        # need to instantiate pre 4.0 router classes
        router = router()
    return router


def prepare(routes):
    """Expand the :setting:`task_routes` setting."""

    def expand_route(route):
        if isinstance(route, (Mapping, list, tuple)):
            return MapRoute(route)
        if isinstance(route, str):
            return mlazy(expand_router_string, route)
        return route

    if routes is None:
        return ()
    if not isinstance(routes, (list, tuple)):
        routes = (routes,)
    return [expand_route(route) for route in routes]
