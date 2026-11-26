import unittest

from golog_gc_request_overlap import exposure, merged


class GCOverlapTests(unittest.TestCase):
    def test_known_inner_outer_and_negative(self):
        events = merged([(140,160),(155,170)])
        self.assertEqual(exposure(35,80,(100,110),events,events),(25,30))
        self.assertEqual(exposure(0,20,(100,110),events,events),(0,0))

    def test_epoch_wider_than_short_request(self):
        self.assertEqual(exposure(20,24,(100,120),[(129,131)],[(127,133)]),(0,4))

    def test_touching_intervals_do_not_overlap(self):
        self.assertEqual(exposure(0,10,(100,100),[(110,111)],[(110,111)]),(0,0))
