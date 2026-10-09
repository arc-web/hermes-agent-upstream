"""Credential-free native limit, cancellation and existing-audit behavior."""
import datetime
import json
import threading
from types import SimpleNamespace
import pytest
from cron.run_limits import BoundedCronRun, validate_limits, audited_daily_tokens

def limits(**updates):
    return dict(daily_tokens=100000,deadline_seconds=900,parent_max_iterations=5,child_max_iterations=1,max_concurrent_children=3,max_total_children=12,child_max_output_tokens=500,child_context_max_chars=2000,estimated_child_tokens=5000,audit_job_ids=['daily','ads'],**updates)

def agent(input=0,output=0):
    stop=threading.Event();calls=[]
    def interrupt(reason,**kwargs):calls.append((reason,kwargs));stop.set()
    return SimpleNamespace(session_prompt_tokens=input,session_completion_tokens=output,session_total_tokens=input+output,interrupt=interrupt,stop=stop,calls=calls)

def state(baseline=0, **kwargs):
    return BoundedCronRun(limits(),agent(),baseline,**kwargs)

def test_strict_trusted_limits_and_no_implicit_enablement():
    assert validate_limits(None,'daily') is None
    assert validate_limits(limits(),'daily')['max_total_children']==12
    for bad in [dict(limits(),daily_tokens=True),dict(limits(),daily_tokens=100001),dict(limits(),max_concurrent_children=4),dict(limits(),deadline_seconds=0),dict(limits(),audit_job_ids=['other']),dict(limits(),unexpected=1),{}]:
        with pytest.raises(ValueError):validate_limits(bad,'daily')

def test_existing_audit_exact_scope_date_and_unknown(tmp_path):
    p=tmp_path/'usage_audit.jsonl';now=datetime.datetime(2026,10,9,tzinfo=datetime.timezone.utc)
    rows=[{'job_id':'daily','ts':'2026-10-09T01:00:00Z','total_tokens':25,'usage_complete':True},{'job_id':'ads','ts':'2026-10-09T02:00:00Z','total_tokens':30,'usage_complete':True},{'job_id':'other','ts':'2026-10-09T02:00:00Z','total_tokens':None},{'job_id':'daily','ts':'2026-10-08T02:00:00Z','total_tokens':999}]
    p.write_text('\n'.join(json.dumps(r) for r in rows));assert audited_daily_tokens(p,['daily','ads'],now)==55
    rows.append({'job_id':'daily','ts':'2026-10-09T03:00:00Z','total_tokens':None});p.write_text('\n'.join(json.dumps(r) for r in rows))
    with pytest.raises(ValueError):audited_daily_tokens(p,['daily','ads'],now)

def test_four_waves_three_children_total_twelve():
    s=state()
    for wave in range(4):
        s.admit_children(3,['x']*3)
        children=[agent(100,20) for _ in range(3)]
        for child in children:s.register_child(child)
        with pytest.raises(ValueError):s.admit_children(1,['x'])
        for child in children:s.finish_child(child)
    with pytest.raises(ValueError):s.admit_children(1,['x'])
    s.parent_finished=True;r=s.receipt();assert r['child_count']==12;assert r['child_tokens']==1440;assert r['total_tokens']==1440

def test_parallel_admission_reserves_estimates():
    s=state(89000);s.admit_children(1,['x']);s.admit_children(1,['x'])
    with pytest.raises(ValueError):s.admit_children(1,['x'])

def test_usage_limit_cancels_parent_and_children_and_blocks_readmission():
    s=state();c=agent();s.admit_children(1,['x']);s.register_child(c);c.session_total_tokens=100000
    with pytest.raises(RuntimeError):s.check()
    assert s.parent.stop.is_set() and c.stop.is_set();assert c.calls[0][1]['hard_cancel'] is True
    with pytest.raises(RuntimeError):s.admit_children(1,['x'])

def test_deadline_is_shared_independent_of_progress():
    clock=[0];s=state(clock=lambda:clock[0]);c=agent();s.admit_children(1,['x']);s.register_child(c);s.start()
    try:
        clock[0]=901
        assert s.parent.stop.wait(2) and c.stop.wait(2)
        with pytest.raises(RuntimeError):s.check()
    finally:s.close()

def test_unknown_or_unsettled_usage_is_not_zero_success():
    s=state();s.parent.session_total_tokens=True
    with pytest.raises(RuntimeError):s.check()
    assert s.receipt()['total_tokens'] is None
    s=state();c=agent(40,2);s.admit_children(1,['x']);s.register_child(c);s.parent_finished=True
    assert s.receipt()['total_tokens'] is None
    s.finish_child(c);s.finish_child(c);assert s.receipt()['total_tokens']==42

def test_context_bound_denies_before_start():
    s=state()
    with pytest.raises(ValueError):s.admit_children(1,['x'*2001])
    assert not s.children and s.spawned==0

def test_native_watchdog_returns_on_absolute_deadline(monkeypatch):
    from cron import scheduler
    a=agent();clock=[0];s=BoundedCronRun(limits(),a,0,clock=lambda:clock[0]);a._bounded_cron_run=s
    started=threading.Event()
    def run(prompt,task_id):started.set();a.stop.wait(3);return {'completed':False,'total_tokens':0}
    a.run_conversation=run
    monkeypatch.setattr(scheduler,'_cron_inactivity_seconds',lambda:0)
    caught=[]
    def execute():
        try:scheduler._run_agent_with_watchdog(a,'bounded',{'schedule':{'kind':'interval'}},'daily','daily','task',None)
        except RuntimeError as exc:caught.append(str(exc))
    t=threading.Thread(target=execute);t.start();assert started.wait(2);clock[0]=901;t.join(2)
    assert not t.is_alive() and a.stop.is_set()
    assert caught or s.stopped.is_set()

def test_native_audit_includes_children_without_double_count(monkeypatch):
    from cron import scheduler
    a=agent(100,10);s=BoundedCronRun(limits(),a,0);a._bounded_cron_run=s;c=agent(200,20);s.admit_children(1,['x']);s.register_child(c);s.finish_child(c);s.parent_finished=True
    rows=[];monkeypatch.setattr(scheduler,'_write_usage_audit',rows.append)
    scheduler._FireAudit({'id':'daily'},'daily','test',a).write({'total_tokens':110},None)
    assert rows[0]['parent_tokens']==110 and rows[0]['child_tokens']==220 and rows[0]['total_tokens']==330
    s.finished.clear();scheduler._FireAudit({'id':'daily'},'daily','test',a).write({},'deadline')
    assert rows[1]['total_tokens'] is None and rows[1]['usage_complete'] is False

def test_scoped_constructor_a_b_a_preserves_unrelated_jobs(tmp_path, monkeypatch):
    from cron import scheduler
    audit=tmp_path/'audit';audit.write_text('')
    monkeypatch.setattr(scheduler,'_usage_audit_path',lambda:audit)
    created=[]
    class Fake:
        def __init__(self,**kwargs):self.kwargs=kwargs;self.session_prompt_tokens=0;self.session_completion_tokens=0;self.session_total_tokens=0;created.append(self)
        def interrupt(self,*args,**kwargs):pass
        def close(self):pass
    for bounded in [True,False,True]:
        setup=scheduler._CronAgentSetup(model='test',runtime={},max_iterations=100)
        job={'id':'daily'}
        if bounded:job['run_limits']=limits()
        a=scheduler._construct_cron_agent(Fake,job,{},setup,workdir=None,session_id='synthetic',session_db=None)
        assert a.kwargs['max_iterations']==(5 if bounded else 100)
        assert isinstance(getattr(a,'_bounded_cron_run',None),BoundedCronRun)==bounded
    assert created[0]._bounded_cron_run is not created[2]._bounded_cron_run

def test_over_budget_blocks_before_native_agent_creation(tmp_path,monkeypatch):
    from cron import scheduler
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    audit=tmp_path/'audit';audit.write_text(json.dumps({'job_id':'daily','ts':now,'total_tokens':100000,'usage_complete':True})+'\n')
    monkeypatch.setattr(scheduler,'_usage_audit_path',lambda:audit)
    def forbidden(**kwargs):raise AssertionError('Agent constructed after stop gate')
    setup=scheduler._CronAgentSetup(model='test',runtime={},max_iterations=100)
    with pytest.raises(RuntimeError):scheduler._construct_cron_agent(forbidden,{'id':'daily','run_limits':limits()},{},setup,workdir=None,session_id='synthetic',session_db=None)

def test_request_builder_stops_before_any_provider_kwargs(monkeypatch):
    from agent import chat_completion_helpers as helpers
    a=agent();clock=[0];s=BoundedCronRun(limits(),a,0,clock=lambda:clock[0]);a._bounded_cron_run=s;clock[0]=901
    def forbidden(*args,**kwargs):raise AssertionError('Request built after shared deadline')
    monkeypatch.setattr(helpers,'_build_api_kwargs_for_mode',forbidden)
    with pytest.raises(RuntimeError):helpers.build_api_kwargs(a,[])


def test_trusted_state_getter_rejects_unconfigured_mock():
    from unittest.mock import MagicMock
    from cron.run_limits import bounded_run_for
    assert bounded_run_for(MagicMock()) is None


def test_native_usage_less_response_cannot_complete_zero():
    from agent.turn_usage import record_response_usage
    a=agent();a.context_compressor=SimpleNamespace();a.session_api_calls=0;a.model='synthetic';a.provider='synthetic'
    s=BoundedCronRun(limits(),a,0);a._bounded_cron_run=s
    record_response_usage(a,SimpleNamespace(usage=None),messages=[],api_call_count=1,api_duration=0,compression_attempts=0,max_compression_attempts=1)
    s.parent_finished=True
    assert a.session_api_calls==1 and s.receipt()['total_tokens'] is None
    assert not s.receipt()['usage_complete'] and s.stopped.is_set()

def test_failed_parent_and_child_terminal_are_unknown():
    from concurrent.futures import Future
    s=state();f=Future();f.set_exception(RuntimeError('synthetic'));s.finish_parent(f)
    assert s.receipt()['total_tokens'] is None
    s=state();c=agent(10,2);s.register_child(c);s.finish_child(c,successful=False);s.parent_finished=True
    assert s.receipt()['total_tokens'] is None

def test_canonical_total_not_reconstructed_from_prompt_output():
    a=agent(10,2);a.session_total_tokens=19;s=BoundedCronRun(limits(),a,0);s.parent_finished=True
    assert s.receipt()['total_tokens']==19


def test_shared_deadline_reaches_native_codex_and_inline_abort(monkeypatch):
    from agent import interrupt_control
    class Native(interrupt_control.InterruptControlMixin): pass
    a=Native();a.session_total_tokens=0;a._hard_interrupt_requested=threading.Event()
    a._execution_thread_id=None;a._active_children_lock=threading.Lock();a._active_children=[];a.quiet_mode=True
    codex=[];inline=[]
    a.api_mode='codex_app_server';a._codex_session=SimpleNamespace(request_interrupt=lambda:codex.append('request_interrupt'))
    a._active_request_abort=lambda reason:inline.append(reason)
    clock=[0];s=BoundedCronRun(limits(),a,0,clock=lambda:clock[0]);clock[0]=901
    with pytest.raises(RuntimeError):s.check()
    assert codex==['request_interrupt'] and inline==['interrupt_abort']
    assert a._hard_interrupt_requested.is_set() and a._interrupt_requested


def test_explicit_incomplete_audit_cannot_reuse_numeric_partial(tmp_path):
    p=tmp_path/'audit';p.write_text(json.dumps({'job_id':'daily','ts':'2026-10-09T00:01:00Z','total_tokens':10,'usage_complete':False}))
    with pytest.raises(ValueError):audited_daily_tokens(p,['daily'],datetime.datetime(2026,10,9,tzinfo=datetime.timezone.utc))


def test_native_codex_usage_less_response_is_unknown():
    from agent.codex_runtime import _record_codex_app_server_usage
    a=agent();a.session_api_calls=0;a._session_db=None;a.model='synthetic';a.provider='synthetic';a.base_url='synthetic';a.context_compressor=None
    s=BoundedCronRun(limits(),a,0);a._bounded_cron_run=s
    assert _record_codex_app_server_usage(a,SimpleNamespace(token_usage_last=None))=={}
    s.parent_finished=True
    assert a.session_api_calls==1 and s.receipt()['total_tokens'] is None and s.stopped.is_set()


def test_contradictory_failed_terminal_is_unknown():
    from concurrent.futures import Future
    s=state();f=Future();f.set_result({'completed':True,'failed':True});s.finish_parent(f)
    assert not s.receipt()['usage_complete'] and s.receipt()['total_tokens'] is None


@pytest.mark.parametrize('marker',[None,'true',False,0])
def test_legacy_missing_or_nontrue_audit_cannot_authorize(tmp_path,marker):
    row={'job_id':'daily','ts':'2026-10-09T00:01:00Z','total_tokens':10}
    if marker is not None:row['usage_complete']=marker
    p=tmp_path/'audit';p.write_text(json.dumps(row))
    with pytest.raises(ValueError):audited_daily_tokens(p,['daily'],datetime.datetime(2026,10,9,tzinfo=datetime.timezone.utc))

def test_failed_audit_is_unknown_despite_true_marker(tmp_path):
    p=tmp_path/'audit';p.write_text(json.dumps({'job_id':'daily','ts':'2026-10-09T00:01:00Z','total_tokens':10,'usage_complete':True,'error':'synthetic'}))
    with pytest.raises(ValueError):audited_daily_tokens(p,['daily'],datetime.datetime(2026,10,9,tzinfo=datetime.timezone.utc))
