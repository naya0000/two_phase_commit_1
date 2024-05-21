from concurrent import futures
from parameterized import parameterized
import itertools
import logging
import os.path
import random
import tempfile
import unittest

import grpc
import worker
import coordinator
import persistent_log

import twophase_pb2
import twophase_pb2_grpc

from testing_forwarder import TestingForwarder
import test_util

logger = logging.getLogger(__name__)

class InjectedFailuresTest(test_util.TestBase):
    def _set_one_value_no_failures(self, worker_count=1):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')
    
    def _change_value_no_failures(self, worker_count=1):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.check_values('ValueB')

    def _change_value_one_failure(self, worker_count=2, failed_index=1, fail_after=1, but_send=False):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.worker_forwarders[failed_index].fail_after(fail_after, but_send=but_send)
        self.do_set_value('ValueB', maybe_fail=True, expect_transition_from='ValueA')
        self.worker_forwarders[failed_index].stop_failing()
        self.recover_coordinator()
        self.check_values('ValueA', 'ValueB')

    def _change_value_with_reordering(self, worker_count=2, failed_index=1, fail_after=1):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.worker_forwarders[failed_index].delay_after(fail_after)
        self.do_set_value('ValueB', maybe_fail=True, expect_transition_from='ValueA')
        self.worker_forwarders[failed_index].replay_delayed()
        self.recover_coordinator(maybe_fail=True)
        self.worker_forwarders[failed_index].stop_replaying_delayed()
        self.recover_coordinator()
        self.check_values('ValueA', 'ValueB')
        self.do_set_value('ValueC')
        self.check_values('ValueC')

    def _change_value_a_lot_with_reordering(self, worker_count=2, failed_index=1, fail_after=1):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.worker_forwarders[failed_index].delay_after(fail_after)
        self.do_set_value('ValueB', maybe_fail=True, expect_transition_from='ValueA')
        self.worker_forwarders[failed_index].stop_delaying()
        self.recover_coordinator()
        self.do_set_value('ValueC')
        self.worker_forwarders[failed_index].replay_delayed()
        self.do_set_value('ValueD', maybe_fail=True)
        self.recover_coordinator(maybe_fail=True)
        self.worker_forwarders[failed_index].stop_replaying_delayed()
        self.recover_coordinator()
        self.check_values('ValueC', 'ValueD')
        self.do_set_value('ValueE')
        self.check_values('ValueE')

    def _double_reordering_over_recovery(self, worker_count=2, failed_index_1=0, failed_index_2=1,
                                                   fail_after_1=False, fail_after_2=False):
        self.start_coordinator_and_workers(worker_count)
        self.do_set_value('ValueA')
        self.worker_forwarders[failed_index_1].delay_after(fail_after_1)
        self.worker_forwarders[failed_index_2].delay_after(fail_after_2)
        self.do_set_value('ValueB', maybe_fail=True, expect_transition_from='ValueA')
        self.check_values('ValueA', 'ValueB', allow_unavailable=True)
        self.worker_forwarders[failed_index_1].stop_delaying()
        self.recover_coordinator(maybe_fail=True)
        self.check_values('ValueA', 'ValueB', allow_unavailable=True)
        self.worker_forwarders[failed_index_2].stop_delaying()
        self.recover_coordinator()
        self.check_values('ValueA', 'ValueB')
        self.do_set_value('ValueC')
        self.worker_forwarders[failed_index_1].replay_delayed()
        self.do_set_value('ValueD', expect_transition_from='ValueC')
        self.recover_coordinator(maybe_fail=True)
        self.check_values('ValueC', 'ValueD', allow_unavailable=True)
        self.worker_forwarders[failed_index_2].replay_delayed()
        self.recover_coordinator(maybe_fail=True)
        self.worker_forwarders[failed_index_1].stop_replaying_delayed()
        self.worker_forwarders[failed_index_2].stop_replaying_delayed()
        self.check_values('ValueC', 'ValueD', allow_unavailable=True)
        self.recover_coordinator()
        self.check_values('ValueC', 'ValueD')
        self.do_set_value('ValueE')
        self.check_values('ValueE')


    @parameterized.expand([(1,), (2,), (5,)])
    def test_set_one_value_no_failures(self, workers):
        '''Set one value with no failures, variable number of workers.'''
        self._set_one_value_no_failures(workers)
  
    @parameterized.expand([(1,), (2,), (5,)]) 
    def test_change_value_no_failures(self, workers):
        self._change_value_no_failures(workers)
   
    @parameterized.expand([
        (1, 0, 0),
        (1, 0, 1),
        (1, 0, 2),
        (1, 0, 3),
        (1, 0, 4),
        (2, 0, 0),
        (2, 1, 0),
        (2, 0, 1),
        (2, 1, 1),
        (2, 0, 2),
        (2, 1, 2),
        (2, 0, 3),
        (2, 1, 3),
        (2, 0, 4),
        (2, 1, 4),
    ]) 
    def test_change_value_reordering(self, worker_count, failed_index, fail_after):
        '''
        Set value twice, with delayed messages from partly-failed first set interfering with second.
        '''
        
        self._change_value_with_reordering(
            worker_count=worker_count,
            failed_index=failed_index,
            fail_after=fail_after
        )
    
    @parameterized.expand([
        (1, 0, 0),
        (1, 0, 1),
        (1, 0, 2),
        (1, 0, 3),
        (1, 0, 4),
        (2, 0, 0),
        (2, 1, 0),
        (2, 0, 1),
        (2, 1, 1),
        (2, 0, 2),
        (2, 1, 2),
        (2, 0, 3),
        (2, 1, 3),
        (2, 0, 4),
        (2, 1, 4),
    ]) 
    def test_change_value_a_lot_with_reordering(self, worker_count, failed_index, fail_after):
        '''
        Set value several times, with delayed messages from partly failed early sets interfering with later operations.
        '''
        self._change_value_a_lot_with_reordering(
            worker_count=worker_count,
            failed_index=failed_index,
            fail_after=fail_after
        )

    @parameterized.expand([
        (2, 0, 1, False, False),
        (2, 0, 1, True, False),
        (2, 0, 1, False, True),
        (2, 0, 1, True, True),
        (3, 0, 1, False, False),
        (3, 0, 1, True, False),
        (3, 0, 1, False, True),
        (3, 0, 1, True, True),
        (3, 0, 2, False, False),
        (3, 0, 2, True, False),
        (3, 0, 2, False, True),
        (3, 0, 2, True, True),
        (5, 1, 4, False, False),
        (5, 1, 4, True, False),
        (5, 1, 4, False, True),
        (5, 1, 4, True, True),
    ]) 
    def test_reordering_over_recovery(self, worker_count, failed_index_1, failed_index_2, fail_after_1, fail_after_2):
        '''
        Try to set value with network failure, then have messages replayed during recover from failure.
        '''
        self._double_reordering_over_recovery(
            worker_count=worker_count,
            failed_index_1=failed_index_1,
            failed_index_2=failed_index_2,
            fail_after_1=fail_after_1,
            fail_after_2=fail_after_2,
        )
    
    @parameterized.expand([
        (2, 0, 0, False),
        (2, 0, 0, True),
        (2, 0, 1, False),
        (2, 0, 1, True),
        (2, 0, 2, False),
        (2, 0, 2, True),
        (2, 1, 0, False),
        (2, 1, 0, True),
        (2, 1, 1, False),
        (2, 1, 1, True),
        (2, 1, 2, False),
        (2, 1, 2, True),
        (5, 1, 0, False),
        (5, 1, 1, False),
        (5, 1, 2, False),
        (5, 0, 0, False),
        (5, 0, 1, False),
        (5, 0, 2, False),
        (5, 3, 0, False),
        (5, 3, 1, False),
        (5, 3, 2, False),
    ]) 
    def test_change_value_one_failure(self, worker_count, failed_index, fail_after, but_send):
        '''Change value with one worker failing to receive/respond in the middle.'''
        self._change_value_one_failure(worker_count=worker_count, failed_index=failed_index, fail_after=fail_after, but_send=but_send)

    def test_coordinator_propogates_worker_failure(self):
        self.start_coordinator_and_workers(1)
        self.do_set_value('ValueA')
        self.worker_forwarders[0].fail_after(0)
        self.do_set_value('ValueB', expect_fail=True)
        self.worker_forwarders[0].stop_failing()
    
    def test_coordinator_propogates_worker_log_failure_1(self):
        self.start_coordinator_and_workers(1)
        self.do_set_value('ValueA')
        self.worker_logs[0].fail_after(0)
        self.do_set_value('ValueB', expect_fail=True)
        self.worker_logs[0].stop_failing()
    
    def test_coordinator_propogates_worker_log_failure_2(self):
        self.start_coordinator_and_workers(1)
        self.do_set_value('ValueA')
        self.worker_logs[0].fail_after(0, True)
        self.do_set_value('ValueB', expect_fail=True)
        self.worker_logs[0].stop_failing()
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main(verbosity=2)
