import time
import shelve
import logging
import threading
import collections

from functools import partial

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback

from flower.utils.receiver import CustomEventReceiver
from celery.events.state import State
from tornado.options import options

from . import api

from collections import Counter

from prometheus_client import Counter as PrometheusCounter, Histogram, Gauge
from readerwriterlock import rwlock

logger = logging.getLogger(__name__)

prometheus_metrics = None


def get_prometheus_metrics():
    global prometheus_metrics
    if prometheus_metrics is None:
        prometheus_metrics = PrometheusMetrics()

    return prometheus_metrics


class PrometheusMetrics(object):

    def __init__(self):
        self.events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])

        self.runtime = Histogram(
            'flower_task_runtime_seconds',
            "Task runtime",
            ['worker', 'task'],
            buckets=options.task_runtime_metric_buckets
        )
        self.prefetch_time = Gauge(
            'flower_task_prefetch_time_seconds',
            "The time the task spent waiting at the celery worker to be executed.",
            ['worker', 'task']
        )
        self.number_of_prefetched_tasks = Gauge(
            'flower_worker_prefetched_tasks',
            'Number of tasks of given type prefetched at a worker',
            ['worker', 'task']
        )
        self.worker_online = Gauge('flower_worker_online', "Worker online status", ['worker'])
        self.worker_number_of_currently_executing_tasks = Gauge(
            'flower_worker_number_of_currently_executing_tasks',
            "Number of tasks currently executing at a worker",
            ['worker']
        )


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)
        self.counter_mutex = threading.Lock()
        self.metrics = get_prometheus_metrics()
        # Lock its state during handling
        self.lock = rwlock.RWLockFairD()

    def resize_max_workers(self, max_workers):
        assert max_workers > 0, max_workers
        logger.debug(
            "previous max_workers={}, new max_workers={}".format(
                self.workers.limit, max_workers))
        self.workers.limit = max_workers
        if len(self.workers.data) > self.workers.limit:
            for _ in range(len(self.workers.data) - self.workers.limit):
                self.workers.popitem(last=False)

    def resize_max_tasks(self, max_tasks):
        assert max_tasks > 0, max_tasks
        logger.debug(
            "previous max_tasks={}, new max_tasks={}".format(
                self.tasks.limit, max_tasks))
        self.tasks.limit = max_tasks
        if len(self.tasks.data) > self.tasks.limit:
            index = 0
            for _ in range(len(self.tasks.data) - self.tasks.limit):
                index += 1
                self.tasks.popitem(last=False)

    def refresh_worker_state(self):
        if not options.purge_offline_workers:
            return
        logger.info("refresh_worker_state")
        workers = {}
        with self.counter_mutex:
            for name, values in self.counter.items():
                if name not in self.workers:
                    continue
                worker = self.workers[name]
                info = dict(values)
                info.update(status=worker.alive,
                            heartbeats=worker.heartbeats)
                workers[name] = info

        timestamp = int(time.time())

        def worker_timeout(last_heartbeat, timestamp):
            return not last_heartbeat or abs(timestamp - last_heartbeat) > options.purge_offline_workers

        for name, info in workers.items():
            heartbeats = info.get('heartbeats', [])
            last_heartbeat = int(max(heartbeats)) if heartbeats else None
            worker_status = info.get('status', True)
            if worker_timeout(last_heartbeat, timestamp) or not worker_status:
                logger.info("worker[{}] offline because of last_heartbeat={}, timestamp={} or status={}".format(
                    name, last_heartbeat, timestamp, worker_status
                ))
                # If worker is timeout or its status is False, we think it is offline.
                self.metrics.worker_online.labels(name).set(0)
                self.metrics.worker_number_of_currently_executing_tasks.labels(name).set(0)
            else:
                logger.debug("worker[{}] online because of last_heartbeat={}, timestamp={} or status={}".format(
                    name, last_heartbeat, timestamp, worker_status
                ))
                self.metrics.worker_online.labels(name).set(1)

    def event(self, event):
        # Save the event
        with self.lock.gen_rlock():
            super(EventsState, self).event(event)

            worker_name = event['hostname']
            event_type = event['type']

            with self.counter_mutex:
                self.counter[worker_name][event_type] += 1

            if event_type.startswith('task-'):
                task_id = event['uuid']
                task = self.tasks.get(task_id)
                task_name = event.get('name', '')
                if not task_name and task_id in self.tasks:
                    task_name = task.name or ''
                self.metrics.events.labels(worker_name, event_type, task_name).inc()

                runtime = event.get('runtime', 0)
                if runtime:
                    self.metrics.runtime.labels(worker_name, task_name).observe(runtime)

                task_started = task.started
                task_received = task.received

                if event_type == 'task-received' and not task.eta and task_received:
                    self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).inc()

                if event_type == 'task-started' and not task.eta and task_started and task_received:
                    self.metrics.prefetch_time.labels(worker_name, task_name).set(task_started - task_received)
                    self.metrics.number_of_prefetched_tasks.labels(worker_name, task_name).dec()

                if event_type in ['task-succeeded', 'task-failed'] and not task.eta and task_started and task_received:
                    self.metrics.prefetch_time.labels(worker_name, task_name).set(0)

            if event_type == 'worker-online':
                self.metrics.worker_online.labels(worker_name).set(1)

            if event_type == 'worker-heartbeat':
                self.metrics.worker_online.labels(worker_name).set(1)
                logger.debug("worker[{}] heartbeat: {}".format(worker_name, event))

                num_executing_tasks = event.get('active')
                if num_executing_tasks is not None:
                    self.metrics.worker_number_of_currently_executing_tasks.labels(worker_name).set(num_executing_tasks)

            if event_type == 'worker-offline':
                self.metrics.worker_online.labels(worker_name).set(0)



class Events(threading.Thread):
    events_enable_interval = 5000

    def __init__(self, capp, db=None, persistent=False,
                 enable_events=True, io_loop=None, state_save_interval=0,
                 state_update_interval=0,
                 **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop or IOLoop.instance()
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.enable_events = enable_events
        self.state = None
        self.state_save_timer = None
        self.state_update_timer = None

        if self.persistent:
            logger.debug("Loading state from '%s'...", self.db)
            state = shelve.open(self.db)
            if state:
                self.state = state['events']
            state.close()
            max_workers = kwargs.get('max_workers_in_memory')
            if max_workers and self.state:
                self.state.resize_max_workers(max_workers)
            max_tasks = kwargs.get('max_tasks_in_memory')
            if max_tasks and self.state:
                self.state.resize_max_tasks(max_tasks)

            if state_save_interval:
                self.state_save_timer = PeriodicCallback(self.save_state,
                                                         state_save_interval)

        if not self.state:
            self.state = EventsState(**kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

        if state_update_interval:
            self.state_update_timer = PeriodicCallback(self.update_state,
                                                       state_update_interval)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

        if self.state_save_timer:
            logger.debug("Starting state save timer...")
            self.state_save_timer.start()

        if self.state_update_timer:
            logger.debug("Starting state update timer...")
            self.state_update_timer.start()

    def stop(self):
        if self.enable_events:
            logger.debug("Stopping enable events timer...")
            self.timer.stop()

        if self.state_save_timer:
            logger.debug("Stopping state save timer...")
            self.state_save_timer.stop()

        if self.state_update_timer:
            logger.debug("Stopping state update timer...")
            self.state_update_timer.stop()

        if self.persistent:
            self.save_state()

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.capp.connection() as conn:
                    # worker events can be ignored.
                    recv = CustomEventReceiver(
                        conn, handlers={"*": self.on_event}, app=self.capp,
                        prefetch_count=options.task_event_prefetch_count,
                    )
                    recv.bind_queue(
                        options.worker_event_queue, "worker.#",
                        queue_ttl=options.worker_queue_ttl,
                        queue_expires=options.worker_queue_expires,
                        auto_delete=True, durable=False, no_ack=True,
                    )
                    recv.bind_queue(
                        options.task_event_queue, "task.#",
                        auto_delete=False, durable=True, no_ack=False,
                    )
                    try_interval = 1
                    logger.debug("Capturing events...")
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def save_state(self):
        logger.info("Saving state to '%s'...", self.db)
        shelve_state = shelve.open(self.db, flag='n')
        with self.state.lock.gen_wlock():
            shelve_state['events'] = self.state
        shelve_state.close()
        logger.info("Saving state done")

    def update_state(self):
        self.io_loop.run_in_executor(None, self.state.refresh_worker_state)

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        self.io_loop.run_in_executor(None, self.capp.control.enable_events)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
