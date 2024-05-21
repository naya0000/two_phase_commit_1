import os.path
import tempfile
import unittest

import grpc
import worker
import coordinator
import persistent_log

import twophase_pb2
import twophase_pb2_grpc

import test_util

from parameterized import parameterized

class OneCoordinatorTest(test_util.TestBase):
    @parameterized.expand([
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
    ])
    def test_set_one_value(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')
        self.recover_coordinator()
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
    ])
    def test_change_values(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.do_set_value('ValueB')
        self.check_values('ValueB')
        self.do_set_value('ValueC')
        self.check_values('ValueC')
        self.do_set_value('ValueD')
        self.check_values('ValueD')
        self.do_set_value('ValueE')
        self.check_values('ValueE')
        self.recover_workers()
        self.check_values('ValueE')
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')
    
    @parameterized.expand([
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
    ])
    def test_change_values_with_consistent_transitions(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.check_values('ValueB')
        self.do_set_value('ValueC', expect_transition_from='ValueB')
        self.check_values('ValueC')
        self.do_set_value('ValueD', expect_transition_from='ValueC')
        self.check_values('ValueD')
        self.do_set_value('ValueE', expect_transition_from='ValueD')
        self.check_values('ValueE')
        self.recover_workers()
        self.check_values('ValueE')
        self.do_set_value('ValueA', expect_transition_from='ValueE')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')

if __name__ == '__main__':
    unittest.main(verbosity=2)
