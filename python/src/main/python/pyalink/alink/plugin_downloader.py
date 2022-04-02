from .common.types.bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion


class PluginDownloader(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = "com.alibaba.alink.common.io.plugin.PluginDownloader"

    def __init__(self):
        self._j_obj = self._j_cls()()

    def get_j_obj(self):
        return self._j_obj

    def loadConfig(self, configFileName: str = None):
        if configFileName is None:
            return self.loadConfig()
        else:
            return self.loadConfig(configFileName)

    def listAvailablePlugins(self):
        return self.listAvailablePlugins()

    def listAvailablePluginVersions(self, pluginName: str):
        return self.listAvailablePluginVersions(pluginName)

    def localJarsPluginPath(self, pluginName, pluginVersion):
        return self.localJarsPluginPath(pluginName, pluginVersion)

    def localResourcePluginPath(self, pluginName, pluginVersion):
        return self.localResourcePluginPath(pluginName, pluginVersion)

    def downloadPlugin(self, pluginName: str, pluginVersion: str = None):
        if pluginVersion is None:
            return self.downloadPlugin(pluginName)
        else:
            return self.downloadPlugin(pluginName, pluginVersion)

    def downloadAll(self):
        return self.downloadAll()

    def upgrade(self):
        return self.upgrade()

    _unsupported_j_methods = ['upgradePlugin', 'loadJarsPluginConfig', 'downloadPluginSafely',
                              'loadResourcePluginConfig', 'checkPluginExistRoughly']
