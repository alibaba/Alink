package com.alibaba.alink.common.io.plugin;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

/**
 * This class is used to create a collection of {@link org.apache.flink.core.plugin.PluginDescriptor} based on directory
 * structure for a given plugin root folder.
 *
 * <p>The expected structure is as follows: the given plugins root folder, containing the flink version based folder.
 * One flink version folder contains all plugins. One plugin folder contains all resources (jar files) belonging to a
 * plugin. The name of the plugin folder becomes the plugin id.
 * <pre>
 * plugins-root-folder/
 *            |------ flink-1.0
 *            |    |------plugina-version/ (folder of plugin a)
 *            |    |   |-plugin-a-1.jar (the jars containing the classes of plugin a)
 *            |    |   |-plugin-a-2.jar
 *            |    |   |-...
 *            |    |------pluginb-version/
 *            |    |   |-plugin-b-1.jar
 *            |------ flink-1.x
 *            |    |------plugina-version/ (folder of plugin a)
 *           ...       |-...
 * </pre>
 */
public class JarsPluginDirectory {

	private final static String PLUGIN_VERSION_FILE_NAME = ".version";
	private final static String PLUGIN_PREFIX = "plugin";
	private final static String PLUGIN_VERSION = "version";
	private final static String PLUGIN_PARENT_FIREST = "parent-first-patterns";
	private final static String PLUGIN_DEFAULT_PARENT_FIRST = "";

	/**
	 * Pattern to match jar files in a directory.
	 */
	private static final String JAR_MATCHER_PATTERN = "glob:**.jar";

	private final Path pluginsRootDir;

	/**
	 * Matcher for jar files in the filesystem of the root folder.
	 */
	private final PathMatcher jarFileMatcher;

	private static final Logger LOG = LoggerFactory.getLogger(JarsPluginDirectory.class);

	public JarsPluginDirectory(Path pluginsRootDir) {
		this.pluginsRootDir = pluginsRootDir;

		this.jarFileMatcher = pluginsRootDir.getFileSystem().getPathMatcher(JAR_MATCHER_PATTERN);
	}

	public JarsPluginDescriptor createPluginDescriptorForSubDirectory(
		String flinkVersion, String pluginName, String pluginVersion) throws IOException {

		Path subDirectory = Paths.get(
			pluginsRootDir.toString(), flinkVersion, String.format("%s-%s", pluginName, pluginVersion)
		);

		URL[] urls = createJarURLsFromDirectory(subDirectory);
		Arrays.sort(urls, Comparator.comparing(URL::toString));
		Path versionFile = Paths.get(subDirectory.toString(), PLUGIN_VERSION_FILE_NAME);
		String version = pluginVersion;
		String parentFirst = PLUGIN_DEFAULT_PARENT_FIRST;

		if (Files.exists(versionFile) && Files.isRegularFile(versionFile, LinkOption.NOFOLLOW_LINKS)) {
			Properties properties = new Properties();
			try (FileInputStream inputStream = new FileInputStream(versionFile.toFile())) {
				properties.load(inputStream);
			}

			version = properties.getProperty(Joiner.on(".").join(PLUGIN_PREFIX, PLUGIN_VERSION), version);
			parentFirst = properties.getProperty(Joiner.on(".").join(PLUGIN_PREFIX, PLUGIN_PARENT_FIREST),
				parentFirst);
		}

		return new JarsPluginDescriptor(
			subDirectory.getFileName().toString(),
			urls,
			parentFirst.isEmpty()
				? new String[0]
				: parentFirst.split(";"),
			version);
	}

	private URL[] createJarURLsFromDirectory(Path subDirectory) throws IOException {
		URL[] urls = Files.list(subDirectory)
			.filter((Path p) -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) && jarFileMatcher.matches(p))
			.map((Path p) -> {
				try {
					URL url = p.toUri().toURL();
					LOG.info("Found jar url: {}", url);
					return url;
				} catch (MalformedURLException e) {
					throw new RuntimeException(e);
				}
			})
			.toArray(URL[]::new);

		if (urls.length < 1) {
			throw new IOException("Cannot find any jar files for plugin in directory [" + subDirectory + "]." +
				" Please provide the jar files for the plugin or delete the directory.");
		}

		return urls;
	}
}
