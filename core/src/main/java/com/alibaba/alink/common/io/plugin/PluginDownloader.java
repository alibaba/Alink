package com.alibaba.alink.common.io.plugin;

import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.DistributePluginException;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.pyrunner.TarFileUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PluginDownloader {

	private final static Logger LOG = LoggerFactory.getLogger(PluginDownloader.class);

	private Map <String, PluginDownloaderConfig> jarsPluginConfigs;
	private boolean isJarsPluginConfigLoaded = false;

	private Map <String, PluginDownloaderConfig> resourcePluginConfigs;
	private boolean isResourcePluginConfigLoaded = false;

	private final FilePath sourceRoot;

	private String pluginDir;

	public PluginDownloader() {
		this(null);
	}

	public PluginDownloader(String pluginDir) {
		this(new FilePath(AlinkGlobalConfiguration.getPluginUrl()), pluginDir);
	}

	public PluginDownloader(String sourceRoot, String pluginDir) {
		this(new FilePath(sourceRoot), pluginDir);
	}

	public PluginDownloader(FilePath sourceRoot, String pluginDir) {
		this.sourceRoot = sourceRoot;
		this.pluginDir = pluginDir;
	}

	public void loadConfig() throws IOException {
		loadPluginConfig();
	}

	public List <String> listAvailablePlugins() throws IOException {
		loadPluginConfig();

		List <String> all = new ArrayList <>(jarsPluginConfigs.keySet());
		all.addAll(resourcePluginConfigs.keySet());

		return Collections.unmodifiableList(all);
	}

	public List <String> listAvailablePluginVersions(String pluginName) throws IOException {
		loadPluginConfig();

		if (jarsPluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> allVersions = jarsPluginConfigs.get(pluginName).versions;
			return new ArrayList <>(allVersions.keySet());
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> allVersions = resourcePluginConfigs.get(pluginName).versions;
			return new ArrayList <>(allVersions.keySet());
		} else {
			throw new AkIllegalOperatorParameterException("plugin [" + pluginName + "] not found!");
		}
	}

	public Path localJarsPluginPath(String pluginName, String pluginVersion) {
		return new Path(getLocalFlinkRoot(), pluginName + "-" + pluginVersion);
	}

	public Path localResourcePluginPath(String pluginName, String pluginVersion) {
		return new Path(getLocalResourceRoot(), pluginName + "-" + pluginVersion);
	}

	public boolean checkPluginExistRoughly(String pluginName, String pluginVersion) {

		Path localPath = localJarsPluginPath(pluginName, pluginVersion);

		if (new File(localPath.getPath()).exists()) {
			LOG.info("Found jars plugin: {}", localPath);
			return true;
		}

		localPath = localResourcePluginPath(pluginName, pluginVersion);

		if (new File(localPath.getPath()).exists()) {
			LOG.info("Found resource plugin: {}", localPath);
			return true;
		}

		return false;
	}

	public void downloadPlugin(String pluginName, String pluginVersion) throws IOException {
		loadPluginConfig();

		String pluginFolder = pluginName + "-" + pluginVersion;

		if (jarsPluginConfigs.containsKey(pluginName)) {
			List <String> jars = getListOfJars(pluginName, pluginVersion);
			FilePath remotePath = new FilePath(
				new Path(getRemoteFlinkRoot().getPath(), pluginFolder),
				getRemoteFlinkRoot().getFileSystem()
			);
			Path localPath = new Path(getLocalFlinkRoot(), pluginFolder);
			for (String jar : jars) {
				download(
					new FilePath(new Path(remotePath.getPath(), jar), remotePath.getFileSystem()),
					new Path(localPath, jar).getPath()
				);
			}
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			List <String> resources = getListOfResource(pluginName, pluginVersion);
			FilePath remotePath = new FilePath(
				new Path(getRemoteResourceRoot().getPath(), pluginFolder),
				getRemoteResourceRoot().getFileSystem()
			);
			Path localPath = new Path(getLocalResourceRoot(), pluginFolder);
			for (String resource : resources) {
				download(
					new FilePath(new Path(remotePath.getPath(), resource), remotePath.getFileSystem()),
					new Path(localPath, resource).getPath()
				);
			}
		} else {
			throw new AkIllegalOperatorParameterException("plugin [" + pluginName + "] not found!");
		}
	}

	public void downloadPluginSafely(String pluginName, String pluginVersion) throws IOException {
		loadPluginConfig();

		String pluginFolder = pluginName + "-" + pluginVersion;

		LOG.info(
			"Plugin: attempt to download {}\njars config: {}\nresource config: {}",
			pluginFolder,
			JsonConverter.toJson(jarsPluginConfigs),
			JsonConverter.toJson(resourcePluginConfigs)
		);

		if (jarsPluginConfigs.containsKey(pluginName)) {
			final List <String> jars = getListOfJars(pluginName, pluginVersion);

			LOG.info("Attempt to downloads: {}", JsonConverter.toJson(jars));

			final FilePath remotePath = new FilePath(
				new Path(getRemoteFlinkRoot().getPath(), pluginFolder),
				getRemoteFlinkRoot().getFileSystem()
			);

			downloadFileLocked(getLocalFlinkRoot(), pluginFolder, (rawPath, path) -> {

				if (new File(rawPath.getPath()).exists()) {
					return;
				}

				File f = new File(path.getPath());

				if (f.exists()) {

					// overwrite tmp file.
					LOG.info("Tmp file {} exists. Delete first.", f);

					try {
						FileUtils.forceDelete(f);
					} catch (IOException e) {
						LOG.warn("Delete tmp file {} returns false.", f, e);
					}
				}

				for (String jar : jars) {

					LOG.info("Attempt to download: {}", jar);

					download(
						new FilePath(new Path(remotePath.getPath(), jar), remotePath.getFileSystem()),
						new Path(path, jar).getPath()
					);
				}

				if (!new File(path.getPath()).renameTo(new File(rawPath.getPath()))) {
					throw new DistributePluginException(String.format("Commit file: %s fail.", path.getPath()));
				}
			});

		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			final List <String> resources = getListOfResource(pluginName, pluginVersion);

			LOG.info("Attempt to downloads: {}", JsonConverter.toJson(resources));

			final FilePath remotePath = new FilePath(
				new Path(getRemoteResourceRoot().getPath(), pluginFolder),
				getRemoteResourceRoot().getFileSystem()
			);

			downloadFileLocked(getLocalResourceRoot(), pluginFolder, (rawPath, path) -> {

				if (new File(rawPath.getPath()).exists()) {
					return;
				}

				File f = new File(path.getPath());

				if (f.exists()) {

					// overwrite tmp file.
					LOG.info("Tmp file {} exists. Delete first.", f);

					try {
						FileUtils.forceDelete(f);
					} catch (IOException e) {
						LOG.warn("Delete tmp file {} returns false.", f, e);
					}
				}

				for (String resource : resources) {

					LOG.info("Attempt to download: {}", resource);

					download(
						new FilePath(new Path(remotePath.getPath(), resource), remotePath.getFileSystem()),
						new Path(path, resource).getPath()
					);
				}

				if (!new File(path.getPath()).renameTo(new File(rawPath.getPath()))) {
					throw new DistributePluginException(String.format("Commit file: %s fail.", path.getPath()));
				}
			});

		} else {
			throw new DistributePluginException("plugin [" + pluginName + "] not found!");
		}
	}

	public void downloadPlugin(String pluginName) throws IOException {
		loadPluginConfig();

		downloadPlugin(pluginName, getDefaultPluginVersion(pluginName));
	}

	public void downloadAll() throws IOException {
		List <String> plugins = this.listAvailablePlugins();
		for (String plugin : plugins) {
			downloadPlugin(plugin);
		}
	}

	/**
	 * Scans all existing plugins and re-download them more carefully.
	 */
	public void upgrade() throws IOException {
		loadPluginConfig();

		File root = new File(getLocalFlinkRoot());
		File[] pluginFiles = root.listFiles();

		if (pluginFiles != null) {
			upgradePlugin(pluginFiles);
		}

		root = new File(getLocalResourceRoot());
		pluginFiles = root.listFiles();

		if (pluginFiles != null) {
			upgradePlugin(pluginFiles);
		}
	}

	public void upgradePlugin(File[] pluginFiles) throws IOException {
		// backup and download all plugins
		for (File pluginFile : pluginFiles) {
			String pluginDirName = pluginFile.getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			String pluginName = pluginDirName.substring(0, dashIndex);
			String pluginVersion = pluginDirName.substring(dashIndex + 1);
			backupPlugin(pluginFile);
			downloadPlugin(pluginName, pluginVersion);
		}

		// delete all backups
		for (File pluginFile : pluginFiles) {
			String pluginDirName = pluginFile.getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			deletePlugin(new File(pluginFile.getAbsolutePath() + ".old"));
		}
	}

	private void loadPluginConfig() throws IOException {
		if (!isJarsPluginConfigLoaded) {
			loadJarsPluginConfig();
		}

		if (!isResourcePluginConfigLoaded) {
			loadResourcePluginConfig();
		}
	}

	private String getLocalFlinkRoot() {
		return new Path(getPluginDir(), AlinkGlobalConfiguration.getFlinkVersion()).toString();
	}

	private FilePath getRemoteFlinkRoot() {
		return new FilePath(
			new Path(sourceRoot.getPath(), AlinkGlobalConfiguration.getFlinkVersion()),
			sourceRoot.getFileSystem()
		);
	}

	private String getLocalResourceRoot() {
		return new Path(getPluginDir(), ResourcesPluginDirectory.RESOURCE_FOLDER).getPath();
	}

	private FilePath getRemoteResourceRoot() {
		return new FilePath(
			new Path(sourceRoot.getPath(), ResourcesPluginDirectory.RESOURCE_FOLDER),
			sourceRoot.getFileSystem()
		);
	}

	/**
	 * download config.json from remote and load it (force).
	 */
	public void loadJarsPluginConfig() throws IOException {
		jarsPluginConfigs = JsonConverter.fromJson(
			loadPluginConfig(getRemoteFlinkRoot(), getLocalFlinkRoot()),
			new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType()
		);
		isJarsPluginConfigLoaded = true;
	}

	public void loadResourcePluginConfig() throws IOException {
		resourcePluginConfigs = JsonConverter.fromJson(
			loadPluginConfig(getRemoteResourceRoot(), getLocalResourceRoot()),
			new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType()
		);
		isResourcePluginConfigLoaded = true;
	}

	void loadConfigFromString(String jsonString) {
		if (!isJarsPluginConfigLoaded) {
			jarsPluginConfigs = JsonConverter.fromJson(jsonString,
				new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		}
		isJarsPluginConfigLoaded = true;
	}

	void loadResourceConfigFromString(String jsonString) {
		if (!isResourcePluginConfigLoaded) {
			resourcePluginConfigs = JsonConverter.fromJson(jsonString,
				new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		}

		isResourcePluginConfigLoaded = true;
	}

	private String loadPluginConfig(
		FilePath remotePath, String localPath) throws IOException {

		return loadPluginConfig("config.json", remotePath, localPath, true);
	}

	private static String loadPluginConfig(
		String configFileName, FilePath remotePath,
		String localPath, boolean force) throws IOException {

		final FilePath remoteFilePath = new FilePath(
			new Path(remotePath.getPath(), configFileName), remotePath.getFileSystem()
		);

		downloadFileLocked(localPath, configFileName, (rawPath, path) -> {

			File rawFile = new File(rawPath.getPath());

			if (force || !rawFile.exists()) {
				download(remoteFilePath, path.getPath());

				if (force) {
					rawFile.delete();
				}

				if (!new File(path.getPath()).renameTo(new File(rawPath.getPath()))) {
					throw new DistributePluginException(String.format("Commit file: %s fail.", path.getPath()));
				}
			}
		});

		return Files.toString(new File(new Path(localPath, configFileName).getPath()), StandardCharsets.UTF_8);
	}

	private List <String> getListOfResource(String pluginName, String pluginVersion) throws IOException {
		if (!isResourcePluginConfigLoaded) {
			loadResourcePluginConfig();
		}

		if (resourcePluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> versions = resourcePluginConfigs.get(pluginName).versions;
			if (versions.containsKey(pluginVersion)) {
				return versions.get(pluginVersion);
			} else {
				throw new AkIllegalOperatorParameterException(
					"plugin [" + pluginName + "], version [" + pluginVersion + "] not found!");
			}
		} else {
			throw new AkIllegalOperatorParameterException("plugin [" + pluginName + "] not found!");
		}
	}

	private List <String> getListOfJars(String pluginName, String pluginVersion) throws IOException {
		if (!isJarsPluginConfigLoaded) {
			loadJarsPluginConfig();
		}
		if (jarsPluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> versions = jarsPluginConfigs.get(pluginName).versions;
			if (versions.containsKey(pluginVersion)) {
				return versions.get(pluginVersion);
			} else {
				throw new AkIllegalOperatorParameterException(
					"plugin [" + pluginName + "], version [" + pluginVersion + "] not found!");
			}
		} else {
			throw new AkIllegalOperatorParameterException("plugin [" + pluginName + "] not found!");
		}
	}

	private String getDefaultPluginVersion(String pluginName) throws IOException {
		loadPluginConfig();

		if (jarsPluginConfigs.containsKey(pluginName)) {
			return jarsPluginConfigs.get(pluginName).defaultVersion;
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			return resourcePluginConfigs.get(pluginName).defaultVersion;
		} else {
			throw new AkIllegalOperatorParameterException("plugin [" + pluginName + "] not found!");
		}
	}

	private static void download(FilePath filePath, String localPath) throws IOException {

		File f = new File(localPath);
		if (!f.exists()) {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("Downloading %s to %s", filePath.getPathStr(), localPath));
			}

			BaseFileSystem <?> fileSystem = filePath.getFileSystem();

			try (InputStream inputStream = fileSystem.open(filePath.getPath())) {
				FileUtils.copyInputStreamToFile(inputStream, f);
			}

			if (localPath.endsWith(".tar")) {
				TarFileUtil.unTar(f, f.getParentFile(), false);
			} else if (localPath.endsWith(".tar.gz")) {
				TarFileUtil.unTar(f, f.getParentFile(), true);
			} else if (localPath.endsWith(".zip")) {
				ZipFileUtil.unzipFileIntoDirectory(f, f.getParentFile());
			}
		} else {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("%s already downloaded to %s", filePath.getPathStr(), localPath));
			}
		}
	}

	private void deletePlugin(File pluginDir) throws IOException {
		if (pluginDir.exists()) {
			FileUtils.deleteDirectory(pluginDir);
		}
	}

	private void backupPlugin(File pluginDir) throws IOException {
		pluginDir.renameTo(new File(pluginDir.getAbsolutePath() + ".old"));
	}

	private String getPluginDir() {
		if (pluginDir == null) {
			// lazy load the default plugin dir
			pluginDir = AlinkGlobalConfiguration.getPluginDir();
		}

		return pluginDir;
	}

	private interface DoDownload {
		void download(Path rawPath, Path path) throws IOException;
	}

	private synchronized static void downloadFileLocked(
		String root, String filePath, DoDownload doDownload)
		throws IOException {

		Path localRawPath = new Path(root, filePath);

		Path localPath = new Path(root, filePath + ".downloading");

		Path lockFile = new Path(
			System.getProperty("java.io.tmpdir"),
			filePath + ".lock"
		);

		LOG.info("Lock file {} in plugin downloader.", lockFile);

		new File(lockFile.getParent().getPath()).mkdirs();

		FileChannel channel = null;
		FileLock lock = null;

		try {
			channel = new FileOutputStream(lockFile.getPath(), true).getChannel();

			lock = channel.lock();

			doDownload.download(localRawPath, localPath);
		} finally {
			if (lock != null) {
				try {
					lock.release();
				} catch (IOException e) {
					// pass
					LOG.warn("Release file lock fail.", e);
				}
			}

			if (channel != null) {
				try {
					channel.close();
				} catch (IOException e) {
					// pass
					LOG.warn("Close channel fail.", e);
				}
			}
		}
	}
}
