package com.alibaba.alink.common.pyrunner;

import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.common.pyrunner.PyCalcRunner.PY_PYTHON_ENV_FILE_PATH;
import static com.alibaba.alink.common.pyrunner.PyCalcRunner.PY_VIRTUAL_ENV_KEY;

public class PyRunnerUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PyRunnerUtils.class);
	private static final String CALL_CONDA_UNPACK_SCRIPT = "/call_conda_pack.sh";
	private static final String WIN_CALL_CONDA_UNPACK_SCRIPT = "/call_conda_pack.bat";

	synchronized static public void callCondaUnpack(String virtualEnv) {
		if (OsType.WINDOWS.equals((OsUtils.getSystemType()))
			&& (!Files.exists(Paths.get(virtualEnv, "Scripts", "activate.bat"))
			|| !Files.exists(Paths.get(virtualEnv, "Scripts", "conda-unpack.exe")))
		) {
			return;
		} else if (!Files.exists(Paths.get(virtualEnv, "bin", "activate")) ||
			!Files.exists(Paths.get(virtualEnv, "bin", "conda-unpack"))) {
			return;
		}

		String scriptResource;
		String fileSuffix;
		String cmd;
		if (OsType.WINDOWS.equals(OsUtils.getSystemType())) {
			scriptResource = WIN_CALL_CONDA_UNPACK_SCRIPT;
			fileSuffix = ".bat";
			cmd = "cmd.exe";
		} else {
			scriptResource = CALL_CONDA_UNPACK_SCRIPT;
			fileSuffix = ".sh";
			cmd = "/bin/bash";
		}

		Path filePath = PythonFileUtils.createTempFile("call_conda_pack", fileSuffix);
		try (final InputStream is = PyRunnerUtils.class.getResourceAsStream(scriptResource)) {
			AkPreconditions.checkNotNull(is, "Cannot get resource " + scriptResource);
			Files.copy(is, filePath, StandardCopyOption.REPLACE_EXISTING);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					FileUtils.deleteFileOrDirectory(filePath.toFile());
				} catch (IOException e) {
					LOG.info("Failed to delete {}.", filePath.toFile().getAbsolutePath(), e);
				}
			}));
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to copy resource: " + scriptResource);
		}
		String[] args = new String[] {cmd, filePath.toAbsolutePath().toString(), virtualEnv};

		LOG.info("Python cmd: {}", String.join(" ", args));
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("Python cmd: " + String.join(" ", args));
		}
		ProcessBuilder builder = new ProcessBuilder()
			.command(args)
			.directory(new File(virtualEnv));
		ProcessBuilderRunner runner = new ProcessBuilderRunner("conda-unpack", builder);
		try {
			runner.start();
		} catch (Exception e) {
			LOG.info("Call conda-unpack failed, ignore it: {}", e.toString());
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.err.println("Call conda-unpack failed, ignore it: " + e);
			}
		}
		LOG.info("Leave PyUtils.callCondaUnpack");
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.err.println("Leave PyUtils.callCondaUnpack");
		}
	}

	public static SerializableBiFunction <String, String, String> overwriteGetConfigFn(
		SerializableBiFunction <String, String, String> getConfigFn, String... strs) {
		AkPreconditions.checkArgument(strs.length % 2 == 0, "strs must be kv pairs of strings.");
		Map <String, String> overwriteConfig = new HashMap <>();
		for (int i = 0; i < strs.length; i += 2) {
			overwriteConfig.put(strs[i], strs[i + 1]);
		}
		//noinspection Convert2Lambda
		return new SerializableBiFunction <String, String, String>() {
			@Override
			public String apply(String key, String defaultValue) {
				return overwriteConfig.getOrDefault(key, getConfigFn.apply(key, defaultValue));
			}
		};
	}

	public static Path downloadPythonEnvFilePath(FilePath filePath, Path workDir) {
		if (PythonFileUtils.isCompressedFile(filePath.getPathStr())) {
			ArchivesUtils.downloadDecompressToDirectory(filePath, workDir.toFile());
			String dirname = PythonFileUtils.getCompressedFileName(filePath.getPathStr());
			if (Files.exists(workDir.resolve(dirname))) {
				return workDir.resolve(dirname);
			} else if (Files.exists(workDir.resolve("bin"))) {
				return workDir;
			} else {
				throw new AkIllegalOperatorParameterException(
					String.format(
						"Must have a folder named bin or %s after extracting python env compressed file %s.",
						dirname, filePath.serialize()));
			}
		} else if (filePath.getFileSystem() instanceof LocalFileSystem) {
			return Paths.get(filePath.getPath().getPath());
		} else {
			throw new AkUnsupportedOperationException(
				"PythonEnvFilePath must be a compressed file or a local directory.");
		}
	}

	public static Map <String, String> handlePythonEnvFilePath(Map <String, String> runConfig, Path workDir) {
		if (runConfig.containsKey(PY_PYTHON_ENV_FILE_PATH)) {
			String pythonEnvFilePathSerialized = runConfig.get(PY_PYTHON_ENV_FILE_PATH);
			runConfig.remove(PY_PYTHON_ENV_FILE_PATH);
			FilePath pythonEnvFilePath = FilePath.deserialize(pythonEnvFilePathSerialized);
			Path virtualEnvPath = downloadPythonEnvFilePath(pythonEnvFilePath, workDir);
			runConfig.put(PY_VIRTUAL_ENV_KEY, virtualEnvPath.toAbsolutePath().toString());
		}
		return runConfig;
	}

	public static SerializableBiFunction <String, String, String> handlePythonEnvFilePath(
		SerializableBiFunction <String, String, String> getConfigFn, Path workDir) {
		String pythonEnvFilePathSerialized = getConfigFn.apply(PY_PYTHON_ENV_FILE_PATH, null);
		if (null != pythonEnvFilePathSerialized) {
			FilePath pythonEnvFilePath = FilePath.deserialize(pythonEnvFilePathSerialized);
			Path virtualEnvPath = downloadPythonEnvFilePath(pythonEnvFilePath, workDir);
			return overwriteGetConfigFn(getConfigFn,
				PY_PYTHON_ENV_FILE_PATH, null,
				PY_VIRTUAL_ENV_KEY, virtualEnvPath.toAbsolutePath().toString());
		}
		return getConfigFn;
	}
}
