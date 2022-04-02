import unittest

from pyalink.alink import *
from pyalink.alink.config import _get_default_local_ip


class TestConfig(unittest.TestCase):

    def test_get_default_local_ip(self):
        local_ip = _get_default_local_ip()
        print(local_ip)

    def test_global_config(self):
        plugin_dir = "/tmp"
        AlinkGlobalConfiguration.setPluginDir(plugin_dir)
        self.assertEqual(AlinkGlobalConfiguration.getPluginDir(), plugin_dir)

        AlinkGlobalConfiguration.setPrintProcessInfo(True)
        self.assertTrue(AlinkGlobalConfiguration.isPrintProcessInfo())
        AlinkGlobalConfiguration.setPrintProcessInfo(False)
        self.assertFalse(AlinkGlobalConfiguration.isPrintProcessInfo())
        AlinkGlobalConfiguration.getFlinkVersion()
