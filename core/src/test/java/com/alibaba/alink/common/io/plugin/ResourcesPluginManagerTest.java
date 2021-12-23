package com.alibaba.alink.common.io.plugin;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

public class ResourcesPluginManagerTest {

	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		File resourceFolder = folder.newFolder("resources");

		if (!new File(resourceFolder, "a-0.1").mkdirs()) {
			throw new RuntimeException();
		}
	}

	@Test
	public void iterator() throws IOException {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ENV_FLINK_PLUGINS_DIR, folder.getRoot().getPath());

		ResourcesPluginManager pluginManager = PluginUtils.createResourcesPluginManagerFromRootFolder(configuration);

		Iterator <ResourcesPluginDescriptor> iterator = pluginManager.iterator("a", "0.1");

		if (iterator.hasNext()) {
			ResourcesPluginDescriptor pluginDescriptor = iterator.next();
			Assert.assertEquals(
				Paths.get(folder.getRoot().getPath(), "resources", "a-0.1").toString(),
				pluginDescriptor.getRootFolder().getPathStr()
			);
		} else {
			Assert.fail();
		}
	}
}