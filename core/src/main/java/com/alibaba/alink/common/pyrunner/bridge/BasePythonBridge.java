package com.alibaba.alink.common.pyrunner.bridge;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.pyrunner.PyMainHandle;
import com.alibaba.alink.common.pyrunner.TarFileUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class BasePythonBridge {
	private final static Logger LOG = LoggerFactory.getLogger(BasePythonBridge.class);

	public static final String PY_CMD_KEY = "py_cmd";
	public static final String PY_JVM_PORT_KEY = "py_jvm_port";
	public static final String PY_PORT_KEY = "py_port";
	public static final String PY_CONNECT_TIMEOUT_KEY = "py_connect_timeout";
	public static final String PY_READ_TIMEOUT_KEY = "py_read_timeout";
	public static final String PY_TURN_ON_LOGGING_KEY = "py_turn_on_logging";
	public static final String PY_VIRTUAL_ENV_KEY = "py_virtual_env";
	public static final String PY_WORK_DIR_KEY = "py_work_dir";
	public static final String PYTHON_RUNNER_SOURCE_PATH = "python_runner_source_path";

	private final static String ALINK_RUNNER_RESOURCE_NAME = "/alink_open_python_runner.tar.gz";
	private final static String ALINK_RUNNER_DIR = "alink_workspace";
	private final static String TMP_FILE_DIR = "tmp";
	private final static String PATH_SEPARATOR = File.pathSeparator;    // system dependent

	volatile GatewayServer server;
	volatile Process process;
	volatile PyMainHandle app;
	volatile boolean alreadyAddHook = false;
	volatile String rootDir = null;
	Map <String, String> extraEnv = new HashMap <>();

	volatile String pyCmd;
	volatile BiFunction <String, String, String> getParamFn;
	volatile Function <String, File> getCacheFileFn;
	volatile String virtualEnv;

	volatile String pythonRunnerSourcePath; // use python_runner in the source

	private static Thread inheritIO(final InputStream src, final Consumer <String> consumer) {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				Scanner sc = new Scanner(src);
				while (sc.hasNextLine() && !Thread.currentThread().isInterrupted()) {
					consumer.accept(sc.nextLine());
				}
			}
		});
		t.setDaemon(true);
		t.start();
		return t;
	}

	List <String> wrapCmdForWin(List <String> args) {
		if (null == virtualEnv) {
			return args;
		}
		if (OsType.WINDOWS.equals(OsUtils.getSystemType())) {
			List <String> newArgs = new ArrayList <>();
			newArgs.add("cmd");
			newArgs.add("/c");
			String cmd = String.format("%s\\Scripts\\activate & %s\\Scripts\\conda-unpack & %s",
				virtualEnv, virtualEnv,
				String.join(" ", args));
			//            newArgs.add("\"" + cmd + "\"");
			newArgs.add(cmd);
			return newArgs;
		} else {
			return args;
			//List <String> newArgs = new ArrayList <>();
			//newArgs.add("/bin/bash");
			//newArgs.add("-c");
			//String cmd = String.format("source %s/bin/activate & %s/bin/conda-unpack & %s",
			//    virtualEnv, virtualEnv,
			//    String.join(" ", args));
			//newArgs.add("\"" + cmd + "\"");
			//return newArgs;
		}
	}

	int startPyProcess(int jvmPort, int pythonPort) {
		prepareEnv();
		String cmd = getPythonCmd();
		LOG.info("begin to start PythonProcess {} -j {} -p {}", cmd, jvmPort, pythonPort);

		String pyStmt = "from alink.py4j_gateway import main;main()";
		if (OsType.WINDOWS.equals(OsUtils.getSystemType())) {
			pyStmt = "\"" + pyStmt + "\"";
		}

		List <String> args = Arrays.asList(
			cmd, "-c", pyStmt,
			"-j", Integer.toString(jvmPort),
			"-p", Integer.toString(pythonPort)
		);
		List <String> newArgs = wrapCmdForWin(args);
		ProcessBuilder pb = new ProcessBuilder(newArgs);
		pb.directory(new File(rootDir));
		LOG.info("the command is: " + String.join(" ", pb.command()));
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("the command is: " + String.join(" ", pb.command()));
		}

		final Map <String, String> env = pb.environment();
		env.remove("LD_PRELOAD");
		env.put("PYTHONIOENCODING", "utf8");
		updateEnv(env, extraEnv);

		pb.redirectErrorStream(true);
		try {
			this.process = pb.start();
			pythonPort = waitProcessStarted(this.process, "Started Listening On ");
			inheritIO(this.process.getInputStream(), line -> {
				LOG.info("PYTHON: {}", line);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("PYTHON: " + line);
				}
			});
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to start Python process.", e);
		}

		if (!alreadyAddHook) {
			alreadyAddHook = true;
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					stopProcess();
				}
			});
		}
		return pythonPort;
	}

    private void updateEnv(Map<String, String> env, Map<String, String> extra) {
        for (Entry<String, String> e : extra.entrySet()) {
            if (e.getKey().endsWith("PATH")) {
                String old = env.getOrDefault(e.getKey(), "");
                env.put(e.getKey(), e.getValue() + PATH_SEPARATOR + old);
            } else {
                env.put(e.getKey(), e.getValue());
            }
        }
    }

    int waitProcessStarted(Process p, String targetString) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (p.isAlive()) {
            sb.setLength(0);
            // next line
            InputStream in = p.getInputStream();
            int c = 0;
            while ((c = in.read()) != -1) {
                if (c == '\r' || c == '\n') {
                    break;
                }
                sb.append((char) c);
            }
            String line = sb.toString();
            if (!line.isEmpty()) {
				LOG.info("subprocess print: {}", line);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("subprocess print: " + line);
				}
				if (line.startsWith(targetString)) {
					LOG.info("subprocess is started");
					return Integer.parseInt(line.substring(targetString.length()));
				}
			}
        }
        throw new AkUnclassifiedErrorException("the process is terminated.");
    }

    private void stopProcess() {
        if (process == null) {
            return;
        }
        LOG.info("the process's status is {}", process.isAlive());
        process.destroy();
        try {
            process.waitFor(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
        if (process.isAlive()) {
            process.destroyForcibly();
        }
        LOG.info("the process has been killed");
        process = null;
    }

    boolean isRunning() {
        if (process != null && server != null && app != null) {
            return app.check();
        }
        return false;
    }

    private String getPythonCmd() {
		if (StringUtils.isNotBlank(pyCmd)) {
			return pyCmd;
		}
		String alinkPythonExecutable = System.getenv("ALINK_PYTHON_EXECUTABLE");
		if (StringUtils.isNotBlank(alinkPythonExecutable)) {
			return alinkPythonExecutable;
		}
		boolean hasPython3 = false;
		try {
			ProcessBuilder processBuilder = new ProcessBuilder()
				.command("python3", "-V")
				.redirectOutput(ProcessBuilder.Redirect.PIPE);
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			String output = IOUtils.toString(inputStream);
			LOG.info("python3 -V has output " + output);
			hasPython3 = (output.startsWith("Python 3."));
		} catch (IOException e) {
			LOG.info("Cannot execute python3 -V", e);
		}
		// Check /usr/ali/python3.7/bin/python3 on Odps
		{
			Path path = Paths.get("/usr/ali/python3.7/bin/python3");
			if (Files.exists(path) && !Files.isDirectory(path)) {
				return path.toString();
			}
		}
		return hasPython3 ? "python3" : "python";
	}

    private void prepareEnv() {
		if (rootDir == null) {
			try {
				rootDir = PythonFileUtils.createTempDir("tmp_py_").toString();
			} catch (Exception e) {
				throw new AkUnclassifiedErrorException("Failed to create temporary directory");
			}
			Paths.get(rootDir, TMP_FILE_DIR).toFile().mkdirs();
			Paths.get(rootDir, ALINK_RUNNER_DIR).toFile().mkdirs();
		}
		setupPythonBaseEnv();
		setupPythonRunner();
	}

	private String getTmpFileDir() {
		File file = Paths.get(rootDir, TMP_FILE_DIR).toFile();
		//noinspection ResultOfMethodCallIgnored
		file.mkdirs();
		return file.getAbsolutePath();
	}

	private void setupPythonBaseEnv() {
		final String stateName = ".python_sys_env__";
		if (isFinished(stateName)) {
			LOG.info("the PythonBaseEnv is already prepared.");
			// If PATH is set, '/usr/bin/env' cannot be found when starting python process.
			// addPathToEnv("PATH", Paths.get(getAlinkSysDir(), "bin").toFile().getAbsolutePath());
			return;
		}
		if (null != virtualEnv) {
			if (OsType.WINDOWS.equals((OsUtils.getSystemType()))) {
				pyCmd = Paths.get(virtualEnv, "python").toFile().getAbsolutePath();
			} else {
				pyCmd = Paths.get(virtualEnv, "bin", "python").toFile().getAbsolutePath();
			}
			LOG.info(String.format("Use Python cmd %s", pyCmd));
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("Use Python cmd " + pyCmd);
			}
		} else {
			LOG.info("No virtual env found, use system Python.");
		}
	}

	private void appendPathToEnv(String key, String path) {
		if (extraEnv.containsKey(key)) {
			extraEnv.put(key, path + PATH_SEPARATOR + extraEnv.get(key));
		} else {
			extraEnv.put(key, path);
		}
	}

	private void addPathToEnv(String key, String path) {
		if (extraEnv.containsKey(key)) {
			String old = extraEnv.get(key);
			if (!Arrays.asList(old.split(PATH_SEPARATOR)).contains(path)) {
				extraEnv.put(key, path + PATH_SEPARATOR + extraEnv.get(key));
			}
		} else {
			extraEnv.put(key, path);
		}
	}

	private void markFinished(String name) {
		name = name.replace('/', '_');
		File stateDir = new File(getTmpFileDir(), ".alink_state");
		stateDir.mkdirs();
		File doneFile = new File(stateDir, name);
		if (!doneFile.exists()) {
			try {
				Files.write(Paths.get(doneFile.getAbsolutePath()), "".getBytes(),
					StandardOpenOption.WRITE,
					StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING);
			} catch (Exception ex) {
				LOG.warn("write file:{} failed", doneFile, ex);
			}
		}
	}

	private boolean isFinished(String name) {
		name = name.replace('/', '_');
		File stateDir = new File(getTmpFileDir(), ".alink_state");
		File doneFile = new File(stateDir, name);
		return doneFile.exists();
	}

	private void setupPythonRunner() {
		if (null != pythonRunnerSourcePath) {
            appendPathToEnv("PYTHONPATH", pythonRunnerSourcePath);
            return;
        }
        String stateName = ".alink_open_python_runner__";
        File dst = Paths.get(rootDir, ALINK_RUNNER_DIR).toFile();
        if (isFinished(stateName)) {
            addPathToEnv("PYTHONPATH", dst.getAbsolutePath());
            LOG.info("the alink_open_python_runner is already prepared.");
            return;
        }
        File f = Paths.get(getTmpFileDir(), "alink_open_python_runner.tar.gz").toFile();
        LOG.info("the tmpFile is: {}", f.getAbsolutePath());
        try (InputStream in = getClass().getResourceAsStream(ALINK_RUNNER_RESOURCE_NAME);
             FileOutputStream fos = new FileOutputStream(f)) {
            assert in != null;
            IOUtils.copyLarge(in, fos);
        } catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to copy alink_open_python_runner.tar.gz.", e);
        }

        try {
            TarFileUtil.unTar(f, dst);
        } catch (IOException e) {
			throw new AkUnclassifiedErrorException(String.format("Failed to un-tar file %s.", f.getAbsolutePath()), e);
        }
        // add path to PYTHONPATH
        appendPathToEnv("PYTHONPATH", dst.getAbsolutePath());
        markFinished(stateName);
    }

    public synchronized void open(String name,
                                  BiFunction <String, String, String> getParamFn,
                                  Function <String, File> getCacheFileFn) {
        LOG.info("open bridge with {}", name);
        if (!isRunning()) {
			int jvmPort = Integer.parseInt(getParamFn.apply(PY_JVM_PORT_KEY, "0"));
			int pythonPort = Integer.parseInt(getParamFn.apply(PY_PORT_KEY, "0"));
			final int pyConnectTimeout = Integer.parseInt(getParamFn.apply(PY_CONNECT_TIMEOUT_KEY, "0"));
			final int pyReadTimeout = Integer.parseInt(getParamFn.apply(PY_READ_TIMEOUT_KEY, "0"));
			final boolean turnOnLogging = Boolean.parseBoolean(getParamFn.apply(PY_TURN_ON_LOGGING_KEY, "false"));
			this.getParamFn = getParamFn;
			this.getCacheFileFn = getCacheFileFn;
			this.rootDir = getParamFn.apply(PY_WORK_DIR_KEY, null);

			this.virtualEnv = getParamFn.apply(PY_VIRTUAL_ENV_KEY, null);
			this.pythonRunnerSourcePath = this.getParamFn.apply(PYTHON_RUNNER_SOURCE_PATH, null);

			this.pyCmd = getParamFn.apply(PY_CMD_KEY, "");

			if (turnOnLogging) {
				GatewayServer.turnLoggingOn();
			} else {
				GatewayServer.turnLoggingOff();
			}

			// Start GatewayServer without CallbackClient, where JVM port could be 0
			server = new GatewayServer(null, jvmPort, pyConnectTimeout, pyReadTimeout);
			server.start();
			// Get the actual JVM port
			jvmPort = server.getListeningPort();
			// Start Python process, where Python port could be 0
			pythonPort = startPyProcess(jvmPort, pythonPort);
			// Start CallbackClient with the actual Python port
			server.resetCallbackClient(GatewayServer.defaultAddress(), pythonPort);

			LOG.info(String.format("JVM port %d, Python port %d", jvmPort, pythonPort));
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.printf("JVM port %d, Python port %d%n", jvmPort, pythonPort);
			}

			app = (PyMainHandle) (server.getPythonServerEntryPoint(new Class[] {
				PyMainHandle.class
			}));
		}
        int n = app.open(name);
        LOG.info("the bridge[{}] is running, concurrent:{}", name, n);
    }

    public synchronized void open(String name, String cmd, int jvmPort, int pythonPort,
                                  int connectTimeout, int readTimeout, boolean turnOnLogging) {
        Map <String, String> config = new HashMap <>();
        config.put(PY_CMD_KEY, cmd);
        config.put(PY_JVM_PORT_KEY, String.valueOf(jvmPort));
        config.put(PY_PORT_KEY, String.valueOf(pythonPort));
        config.put(PY_CONNECT_TIMEOUT_KEY, String.valueOf(connectTimeout));
        config.put(PY_READ_TIMEOUT_KEY, String.valueOf(readTimeout));
        config.put(PY_TURN_ON_LOGGING_KEY, String.valueOf(turnOnLogging));
        open(name, config::getOrDefault, null);
    }

    public synchronized void close(String name) {
        int n = app.close(name);
        LOG.info("close bridge from {}, concurrent:{}", name, n);
        if (app.shutdown(name)) {
            LOG.info("shutdown bridge from {}", name);
            server.shutdown();
            app = null;
            stopProcess();
        }
    }

    public PyMainHandle app() {
        return app;
    }

    public GatewayServer server() {
        return server;
    }
}
