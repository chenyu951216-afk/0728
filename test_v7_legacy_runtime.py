import unittest

import v5_async_runtime as runtime


class Core:
    def __init__(self, version):
        self.state = {'runtime_version': version}


class LegacyRuntimeIsolationTests(unittest.TestCase):
    def test_v7_disables_legacy_boot_and_execution_paths(self):
        self.assertFalse(runtime._legacy_runtime_allowed(Core('7.0.0-20260809')))
        self.assertFalse(runtime._legacy_runtime_allowed(Core('7.1.0')))

    def test_legacy_runtime_remains_available_when_intentional(self):
        self.assertTrue(runtime._legacy_runtime_allowed(Core('6.0.0-20260808')))
        self.assertTrue(runtime._legacy_runtime_allowed(Core('5.0.0-20260808')))


if __name__ == '__main__':
    unittest.main()
