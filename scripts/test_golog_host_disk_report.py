import unittest

from golog_host_clock import offset_bounds
from golog_host_disk_report import clock_projection, match_buckets, whole_buckets


class TimingTest(unittest.TestCase):
    def test_bucket_edges_and_shared_buckets(self):
        buckets = [dict(ix=i, start=[s,s+4], end=[s+40,s+44]) for i,s in enumerate([0,40,80])]
        definite, possible = match_buckets(buckets, [39,42], [41,43])
        self.assertEqual(definite, [])
        self.assertEqual([b['ix'] for b in possible], [0,1])
        definite, _ = match_buckets(buckets, [30,31], [88,89])
        self.assertEqual([b['ix'] for b in definite], [0,1,2])
        self.assertEqual([b['ix'] for b in whole_buckets(buckets, [30,31], [88,89])], [1])
        definite, possible = match_buckets(buckets[:1], [44,44], [46,47])
        self.assertEqual((definite, possible), ([], []))

    def test_round_trips_enclose_known_clock_offset(self):
        def burst(first, when, offset):
            return [dict(token=first+i, guest_send_ns=when+i*100, guest_recv_ns=when+i*100+90,
                         host_before_ns=when+i*100+offset+20, host_after_ns=when+i*100+offset+30)
                    for i in range(12)]

        def clocks(pre, post):
            return dict(pre=pre, post=post, pre_bounds_ns=list(offset_bounds(pre)),
                        post_bounds_ns=list(offset_bounds(post)))

        pre = burst(0,0,1000)
        post = burst(12,10_000_000,1000)
        lo, hi = clock_projection(clocks(pre,post))(5_001_000)
        self.assertLessEqual(lo, 5_000_000)
        self.assertGreaterEqual(hi, 5_000_000)
        with self.assertRaisesRegex(ValueError, 'drift'):
            clock_projection(clocks(pre,burst(12,10_000_000,25_000)))(5_001_000)


if __name__ == '__main__':
    unittest.main()
