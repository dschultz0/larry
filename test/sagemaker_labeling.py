import unittest
import larrydata as ld


class MyTestCase(unittest.TestCase):
    def test_something(self):
        ld.sagemaker.labeling.describe_job('foo')
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
