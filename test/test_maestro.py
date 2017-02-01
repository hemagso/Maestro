import unittest
from maestro import _check_cyclic

class TestCheckCyclicFunction(unittest.TestCase):
    def testAcylic(self):
        self.assertEqual(
            _check_cyclic({0: (), 1: (0,), 2: (1,)}),
            False
        )

    def testCyclic(self):
        self.assertEqual(
            _check_cyclic({0: (2,), 1: (0,), 2: (1,)}),
            True
        )

if __name__ == "__main__":
    unittest.main()