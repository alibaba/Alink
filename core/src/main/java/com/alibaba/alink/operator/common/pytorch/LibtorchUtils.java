package com.alibaba.alink.operator.common.pytorch;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LibtorchUtils {

	private final static Logger LOG = LoggerFactory.getLogger(LibtorchUtils.class);

	static String REGISTER_KEY_TEMPLATE = "libtorch_%s";
	static Map <Pair <OsType, String>, String> LIBTORCH_PATH_MAP = new HashMap <>();

	static {
		LIBTORCH_PATH_MAP.put(Pair.of(OsType.MACOSX, "1.8.1"),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink-plugins/resources/libtorch_macosx-1.8.1/libtorch-macos-1.8.1.zip");
		LIBTORCH_PATH_MAP.put(Pair.of(OsType.LINUX, "1.8.1"),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink-plugins/resources/libtorch_linux-1.8.1/libtorch-shared-with-deps-1.8.1-cpu.zip");
		LIBTORCH_PATH_MAP.put(Pair.of(OsType.WINDOWS, "1.8.1"),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink-plugins/resources/libtorch_windows-1.8.1/libtorch-shared-with-deps-1.8.1-cpu.zip");
	}

	static RegisterKey getRegisterKey(OsType systemType, String pluginVersion) {
		return new RegisterKey(
			String.format(REGISTER_KEY_TEMPLATE, systemType.name().toLowerCase()),
			pluginVersion
		);
	}

	public static RegisterKey getRegisterKey(String pluginVersion) {
		return getRegisterKey(OsUtils.getSystemType(), pluginVersion);
	}

	public static String getLibtorchPath(String pluginVersion) {
		OsType systemType = OsUtils.getSystemType();
		String remotePath = LIBTORCH_PATH_MAP.get(Pair.of(systemType, pluginVersion));

		// Try to get PythonEnv from plugin directory
		FilePath pluginFilePath = null;
		RegisterKey registerKey = getRegisterKey(pluginVersion);
		try {
			pluginFilePath = ResourcePluginFactory.getResourcePluginPath(registerKey);
		} catch (Exception e) {
			String info = String.format("Cannot prepare plugin for %s-%s, fallback to direct downloading from %s.",
				registerKey.getName(), registerKey.getVersion(), remotePath);
			LOG.info(info, e);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(info + ":" + e);
			}
		}
		if (null != pluginFilePath) {
			String directoryName = "libtorch";
			File directoryFile = new File(pluginFilePath.getPath().toString(), directoryName);
			Preconditions.checkArgument(directoryFile.exists(),
				String.format("There should be a directory named %s in plugin directory %s, but cannot be found.",
					directoryName, pluginFilePath.getPath().toString()));
			return directoryFile.getAbsolutePath();
		} else {
			// Download from remote Path
			File tempDir = PythonFileUtils.createTempDir("pytorch_java_lib").toFile();
			ArchivesUtils.downloadDecompressToDirectory(remotePath, tempDir);
			return new File(tempDir, "libtorch").getAbsolutePath();
		}
	}
}
