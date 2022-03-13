import logging
import time

from tornado import web
from tornado import gen

from ..views import BaseHandler
from ..options import options
from ..api.workers import ListWorkers


logger = logging.getLogger(__name__)


class DashboardView(BaseHandler):
    @web.authenticated
    @gen.coroutine
    def get(self):
        refresh = self.get_argument('refresh', default=False, type=bool)
        json = self.get_argument('json', default=False, type=bool)

        events = self.application.events.state

        if refresh:
            try:
                self.application.update_workers()
            except Exception as e:
                logger.exception('Failed to update workers: %s', e)

        workers = {}
        for name, values in events.counter.items():
            if name not in events.workers:
                continue
            worker = events.workers[name]
            info = dict(values)
            info.update(self._as_dict(worker))
            info.update(status=worker.alive)
            workers[name] = info

        if options.purge_offline_workers is not None:
            timestamp = int(time.time())
            offline_workers = []

            def worker_timeout(last_heartbeat, timestamp):
                return not last_heartbeat or abs(timestamp - last_heartbeat) > options.purge_offline_workers

            for name, info in workers.items():
                heartbeats = info.get('heartbeats', [])
                last_heartbeat = int(max(heartbeats)) if heartbeats else None
                worker_status = info.get('status', True)
                if worker_timeout(last_heartbeat, timestamp) or not worker_status:
                    logger.debug("worker[{}] offline because heartbeat timeout, last_heartbeat={}, timestamp={}, or status={}".format(
                        name, last_heartbeat, timestamp, worker_status))
                    offline_workers.append(name)
                else:
                    logger.debug("worker[{}] online because heartbeat timeout, last_heartbeat={}, timestamp={}, or status={}".format(
                        name, last_heartbeat, timestamp, worker_status))

            for name in offline_workers:
                workers.pop(name)

        if json:
            self.write(dict(data=list(workers.values())))
        else:
            self.render("dashboard.html",
                        workers=workers,
                        broker=self.application.capp.connection().as_uri(),
                        autorefresh=1 if self.application.options.auto_refresh else 0)

    @classmethod
    def _as_dict(cls, worker):
        if hasattr(worker, '_fields'):
            return dict((k, worker.__getattribute__(k)) for k in worker._fields)
        else:
            return cls._info(worker)

    @classmethod
    def _info(cls, worker):
        _fields = ('hostname', 'pid', 'freq', 'heartbeats', 'clock',
                   'active', 'processed', 'loadavg', 'sw_ident',
                   'sw_ver', 'sw_sys')

        def _keys():
            for key in _fields:
                value = getattr(worker, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())
