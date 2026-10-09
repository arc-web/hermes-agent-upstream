"""Opt-in limits for one native cron fire; no scheduler or credential store."""
from __future__ import annotations
import datetime
import json
import threading
import time
from pathlib import Path

FIELDS = {'daily_tokens','deadline_seconds','parent_max_iterations','child_max_iterations','max_concurrent_children','max_total_children','child_max_output_tokens','child_context_max_chars','estimated_child_tokens','audit_job_ids'}

def validate_limits(value, job_id):
    if value is None: return None
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise ValueError('run_limits requires the complete trusted limit contract')
    bounds = {'daily_tokens':100000,'deadline_seconds':3600,'parent_max_iterations':12,'child_max_iterations':3,'max_concurrent_children':3,'max_total_children':12,'child_max_output_tokens':2000,'child_context_max_chars':4000,'estimated_child_tokens':10000}
    for key, maximum in bounds.items():
        if type(value[key]) is not int or not 1 <= value[key] <= maximum:
            raise ValueError('Invalid cron run limit: '+key)
    ids=value['audit_job_ids']
    if not isinstance(ids,list) or not ids or len(ids)>16 or any(not isinstance(x,str) or not x or len(x)>128 for x in ids) or job_id not in ids or len(set(ids))!=len(ids):
        raise ValueError('Invalid bounded audit job scope')
    if value['max_concurrent_children']>value['max_total_children']:
        raise ValueError('Concurrent limit exceeds total limit')
    return dict(value, audit_job_ids=list(ids))

def audited_daily_tokens(path: Path, job_ids, now=None):
    now=now or datetime.datetime.now(datetime.timezone.utc)
    day=now.astimezone(datetime.timezone.utc).date()
    if path.stat().st_size>20000000: raise ValueError('Usage audit oversized')
    total=0
    with path.open() as handle:
        for line in handle:
            row=json.loads(line)
            if row.get('job_id') not in job_ids: continue
            stamp=datetime.datetime.fromisoformat(row['ts'].replace('Z','+00:00'))
            if stamp.tzinfo is None: raise ValueError('Usage audit timestamp lacks timezone')
            if stamp.astimezone(datetime.timezone.utc).date()!=day: continue
            if row.get('usage_complete') is False: raise ValueError('Usage audit incomplete')
            tokens=row.get('total_tokens')
            if type(tokens) is not int or tokens<0: raise ValueError('Usage audit UNKNOWN')
            total+=tokens
    return total

class BoundedCronRun:
    """Shared native cancellation and observed-usage guard; in-flight overrun is possible."""
    def __init__(self, limits, parent, baseline, *, clock=time.monotonic):
        if type(baseline) is not int or baseline<0: raise ValueError('Daily usage UNKNOWN')
        self.limits=limits; self.parent=parent; self.baseline=baseline; self.clock=clock
        self.deadline=clock()+limits['deadline_seconds']; self.lock=threading.RLock()
        self.unknown_usage=set();self.children={};self.finished=set();self.spawned=0;self.reason=None;self.parent_finished=False
        self.stopped=threading.Event();self.monitor_stop=threading.Event();self.monitor=None
        self.check()

    def register_child(self, child):
        with self.lock:
            self.check(); self.children[id(child)]=child

    def mark_incomplete(self, agent, reason="Cron usage or terminal outcome UNKNOWN"):
        with self.lock:
            self.unknown_usage.add(id(agent))
        self.stop(reason)

    def finish_parent(self, future):
        try:
            result = future.result()
            successful = isinstance(result, dict) and not result.get("error") and not result.get("failed") and not result.get("interrupted") and result.get("completed") is True
        except BaseException:
            successful = False
        if not successful:
            self.mark_incomplete(self.parent, "Cron parent failed or interrupted")
        self.parent_finished = True

    def finish_child(self, child, successful=True):
        with self.lock:
            self.finished.add(id(child))
            if not successful:
                self.mark_incomplete(child, "Cron child failed or interrupted")

    def observed_usage(self):
        def tokens(agent):
            if id(agent) in self.unknown_usage: raise ValueError('Live usage UNKNOWN')
            values=[getattr(agent,'session_total_tokens',None)]
            if any(type(x) is not int or x<0 for x in values): raise ValueError('Live usage UNKNOWN')
            return sum(values)
        with self.lock:
            parent=tokens(self.parent);children=sum(tokens(x) for x in self.children.values())
            return parent, children

    def stop(self, reason):
        with self.lock:
            if self.stopped.is_set(): return
            self.reason=reason;self.stopped.set();agents=[self.parent,*self.children.values()]
        for agent in agents:
            try: agent.interrupt(reason, hard_cancel=True, tool_reason='bounded_cron_limit')
            except Exception: pass  # stop latch still denies every later admission

    def check(self):
        if self.stopped.is_set(): raise RuntimeError(self.reason)
        if self.clock()>=self.deadline:
            self.stop('Cron shared absolute deadline reached');raise RuntimeError(self.reason)
        try: parent,children=self.observed_usage()
        except ValueError:
            self.stop('Cron live usage UNKNOWN');raise RuntimeError(self.reason)
        if self.baseline+parent+children>=self.limits['daily_tokens']:
            self.stop('Cron observed UTC-day token limit reached');raise RuntimeError(self.reason)

    def admit_children(self, count, contexts):
        with self.lock:
            self.check()
            if type(count) is not int or count<1 or count>self.limits['max_concurrent_children'] or self.spawned+count>self.limits['max_total_children'] or self.spawned-len(self.finished)+count>self.limits['max_concurrent_children']:
                raise ValueError('Cron child count limit reached')
            if any(len(str(x or ''))>self.limits['child_context_max_chars'] for x in contexts):
                raise ValueError('Cron child context exceeds bound')
            parent,children=self.observed_usage()
            if self.baseline+parent+children+(self.spawned-len(self.finished)+count)*self.limits['estimated_child_tokens']>self.limits['daily_tokens']:
                raise ValueError('Insufficient observed daily budget for estimated child batch')
            self.spawned+=count

    def start(self):
        def watch():
            while not self.monitor_stop.wait(min(.1,max(.001,self.deadline-self.clock()))):
                try: self.check()
                except RuntimeError: return
        from agent.memory_provider import spawn_context_thread
        self.monitor=spawn_context_thread(watch,name='cron-bounded-run',daemon=True);self.monitor.start()

    def close(self):
        self.monitor_stop.set()
        if self.monitor is not None: self.monitor.join(timeout=1)

    def receipt(self):
        with self.lock:
            complete=self.parent_finished and set(self.children)==self.finished and self.reason is None
            try: parent,children=self.observed_usage()
            except ValueError: parent=children=None;complete=False
            total=parent+children if complete and parent is not None else None
            return {'parent_tokens':parent,'child_tokens':children,'total_tokens':total,'child_count':len(self.children),'usage_complete':complete,'limit_reason':self.reason,'daily_baseline_tokens':self.baseline,'token_limit_mode':'observed guard plus estimated admission; in-flight overrun possible','deadline_mode':'shared monotonic deadline; native hard-cancel requested; uncooperative work may outlive cancellation'}


def bounded_run_for(agent):
    value = getattr(agent, "_bounded_cron_run", None)
    return value if isinstance(value, BoundedCronRun) else None
