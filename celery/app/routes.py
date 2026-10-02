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
        # Maps each compiled regex back to the original glob/pattern key, so
        # that explain() can report which rule matched in human-readable form.
        self.pattern_sources = {}
        for k, v in map:
            if isinstance(k, Pattern):
                self.patterns[k] = v
                self.pattern_sources[k] = k.pattern
            elif '*' in k:
                regex = re.compile(fnmatch.translate(k))
                self.patterns[regex] = v
                self.pattern_sources[regex] = k
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

    def explain(self, name, *args, **kwargs):
        """Return ``(rule, route)`` describing which rule matched *name*.

        ``rule`` is the exact key or glob pattern that matched, and
        ``route`` is a copy of the configured route.  When no rule
        matches, returns ``(None, None)``.  Read-only: never mutates the
        map or its patterns.
        """
        try:
            return name, dict(self.map[name])
        except KeyError:
            pass
        except ValueError:
            return name, {'queue': self.map[name]}
        for regex, route in self.patterns.items():
            if regex.match(name):
                try:
                    return self.pattern_sources[regex], dict(route)
                except ValueError:
                    return self.pattern_sources[regex], {'queue': route}
        return None, None


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

    def explain(self, name, args=(), kwargs=None, options=None, task_type=None):
        """Explain the routing decision for task *name*.

        Read-only introspection: this never mutates the router, its route
        cache, or the caller's arguments, and never publishes a task.

        Returns a :class:`dict` describing the decision:

          * ``task``           -- the task name.
          * ``matched``        -- whether a configured route matched.
          * ``router_index``   -- index of the matching router in
                                 ``self.routes`` (``None`` when unmatched).
          * ``router``         -- repr of the matching router.
          * ``rule``           -- exact key or glob pattern that matched
                                 (``None`` for non-:class:`MapRoute` routers).
          * ``route``          -- raw route returned by the matching router.
          * ``queue``          -- final resolved queue name.
          * ``default``        -- ``True`` when the default queue was used.
          * ``default_source`` -- origin of the default queue when unmatched.
        """
        kwargs = {} if not kwargs else kwargs
        options = {} if options is None else dict(options)
        info = {
            'task': name,
            'matched': False,
            'router_index': None,
            'router': None,
            'rule': None,
            'route': None,
            'queue': None,
            'default': False,
            'default_source': None,
        }
        route = None
        if self.routes:
            for index, router in enumerate(self.routes):
                rule, candidate = self._explain_router(
                    router, name, args, kwargs, options, task_type)
                if candidate is not None:
                    route = candidate
                    if route:
                        info['matched'] = True
                        info['router_index'] = index
                        info['router'] = repr(router)
                        info['rule'] = rule
                        info['route'] = self._route_copy(route)
                        info['queue'] = self._final_queue_name(route, options)
                    break
        if not route:
            info['default'] = True
            default_queue = self.app.conf.task_default_queue
            if options.get('queue') is not None:
                info['default_source'] = (
                    'options.queue (caller supplied, takes precedence)'
                )
                info['queue'] = self._queue_name(options['queue'])
            else:
                info['default_source'] = (
                    f'task_default_queue={default_queue!r} '
                    f'(app.conf.task_default_queue)'
                )
                info['queue'] = self._queue_name(default_queue)
        return info

    def _explain_router(self, router, name, args, kwargs, options, task_type):
        """Return ``(rule, route)`` for a single router, read-only."""
        router = maybe_evaluate(router)
        if hasattr(router, 'explain'):
            return router.explain(name, *args, **kwargs)
        if hasattr(router, 'route_for_task'):
            route = router.route_for_task(name, args, kwargs)
            return None, route
        route = router(name, args, kwargs, options, task=task_type)
        return None, route

    def _route_copy(self, route):
        """Copy a route dict, leaving strings and other values intact."""
        if isinstance(route, str):
            return route
        return dict(route)

    def _queue_name(self, queue):
        """Return the name of *queue* without mutating the route cache.

        A :class:`Queue` is unwrapped to its name; a string queue name is
        resolved against ``self.queues`` without triggering missing-queue
        creation, so the route cache is never modified.
        """
        if isinstance(queue, Queue):
            return queue.name
        if queue in self.queues:
            return self.queues[queue].name
        return queue

    def _final_queue_name(self, route, options):
        """Compute the final queue name without mutating the route cache.

        Mirrors the precedence in :meth:`route`: a queue supplied in
        *options* wins, otherwise the route's queue is used.
        """
        options_queue = options.get('queue')
        if options_queue is not None:
            return self._queue_name(options_queue)
        if isinstance(route, str):
            return route
        if isinstance(route, Mapping):
            route_queue = route.get('queue')
            if route_queue is not None:
                return self._queue_name(route_queue)
        return None

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
