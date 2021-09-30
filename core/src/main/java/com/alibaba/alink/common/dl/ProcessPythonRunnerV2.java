/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.dl;

import org.apache.flink.util.Preconditions;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.node.runner.python.ProcessPythonRunner;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.alibaba.flink.ml.util.ShellExec;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The runner {@link com.alibaba.flink.ml.cluster.node.runner.python.ProcessPythonRunner} has bugs in
 * checking python environment. This class is a workaround.
 */
public class ProcessPythonRunnerV2 extends ProcessPythonRunner implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessPythonRunnerV2.class);

    private volatile Process child = null;

    private static final String CALL_CONDA_UNPACK_SCRIPT = "/call_conda_pack.sh";

    public ProcessPythonRunnerV2(MLContext MLContext) {
        super(MLContext);
    }

    @Override
    public void runScript() throws IOException {
        String startupScript = mlContext.getProperties().get(MLConstants.STARTUP_SCRIPT_FILE);
        List<String> args = new ArrayList<>();
        String pythonVersion = mlContext.getProperties().getOrDefault(MLConstants.PYTHON_VERSION, "");
        String pythonExec = "python" + pythonVersion;
//		//check if has python2 or python3 environment
//		if (checkPythonEnvironment("which " + pythonExec) != 0){
//			throw new RuntimeException("No this python environment");
//		}
        String virtualEnv = mlContext.getProperties()
            .getOrDefault(MLConstants.VIRTUAL_ENV_DIR, "");
        if (!virtualEnv.isEmpty()) {
            pythonExec = virtualEnv + "/bin/python";
            callCondaUnpack(virtualEnv);
        }
        args.add(pythonExec);
        if (mlContext.startWithStartup()) {
            args.add(startupScript);
            LOG.info("Running {} via {}", mlContext.getScript().getName(), startupScript);
        } else {
            args.add(mlContext.getScript().getAbsolutePath());
        }
        args.add(String.format("%s:%d", mlContext.getNodeServerIP(), mlContext.getNodeServerPort()));
        ProcessBuilder builder = new ProcessBuilder(args);
        builder.environment().clear();
        String classPath = getClassPath();
        if (classPath == null) {
            // can happen in UT
            LOG.warn("Cannot find proper classpath for the Python process.");
        } else {
            mlContext.putEnvProperty(MLConstants.CLASSPATH, classPath);
        }
        if (StringUtils.isNoneEmpty(System.getenv("PATH"))) {
            mlContext.putEnvProperty("PATH", System.getenv("PATH"));
        }
        buildProcessBuilder(builder);
        LOG.info("{} Python cmd: {}", mlContext.getIdentity(), Joiner.on(" ").join(args));
        runProcess(builder);
    }

    /**
     * Conda unpack has to be called for Python environment created by conda-pack. Otherwise, at least OpenSSL won't
     * work correctly.
     * <p>
     * These two commands are called:
     * <pre>
     * source bin/activate
     * bin/conda-unpack
     * </pre>
     * Needs to check this two files are presented.
     * <p>
     */
    synchronized protected void callCondaUnpack(String virtualEnv) throws IOException {
        if (!Files.exists(Paths.get(virtualEnv, "bin", "activate")) ||
            !Files.exists(Paths.get(virtualEnv, "bin", "conda-unpack"))) {
            return;
        }
        InputStream is = this.getClass().getResourceAsStream(CALL_CONDA_UNPACK_SCRIPT);
        Preconditions.checkNotNull(is, "Cannot get resource " + CALL_CONDA_UNPACK_SCRIPT);
        Path filePath = Files.createTempFile("call_conda_pack", ".sh");
        Files.copy(is, filePath, StandardCopyOption.REPLACE_EXISTING);
        String[] args = new String[] {
            "/bin/bash", filePath.toAbsolutePath().toString(), virtualEnv
        };

        LOG.info("{} Python cmd: {}", mlContext.getIdentity(), Joiner.on(" ").join(args));
        System.err.println("Python cmd: " + Joiner.on(" ").join(args));
        ProcessBuilder builder = new ProcessBuilder()
            .command(args)
            .directory(new File(virtualEnv));
        try {
            runProcess(builder);
        } catch (Exception e) {
            LOG.info("Call conda-unpack failed, ignore it: {}", e.toString());
            System.err.println("Call conda-unpack failed, ignore it: " + e);
        }
        LOG.info("Leave ProcessPythonRunnerV2.callCondaUnpack");
        System.err.println("Leave ProcessPythonRunnerV2.callCondaUnpack");
    }

    @Override
    protected void runProcess(ProcessBuilder builder) throws IOException {
        child = builder.start();
        Thread inLogger = new Thread(
            new ShellExec.ProcessLogger(child.getInputStream(), d -> {
                System.out.println(d);
                LOG.info("Python stdout: {}", d);
            }));
        Thread errLogger = new Thread(
            new ShellExec.ProcessLogger(child.getErrorStream(), d -> {
                System.err.println(d);
                LOG.info("Python stderr: {}", d);
            }));
        inLogger.setName(mlContext.getIdentity() + "-in-logger");
        inLogger.setDaemon(true);
        errLogger.setName(mlContext.getIdentity() + "-err-logger");
        errLogger.setDaemon(true);
        inLogger.start();
        errLogger.start();
        try {
            int r = 0;
            do {
                if (child.waitFor(5, TimeUnit.SECONDS)) {
                    r = child.exitValue();
                    break;
                }
            } while (!toKill.get());

            if (r != 0) {
                throw new MLException(
                    String.format("%s python process exited with code %d", mlContext.getIdentity(), r));
            }
        } catch (InterruptedException e) {
            LOG.warn("{} interrupted, killing the process", mlContext.getIdentity());
        } finally {
            killProcess();
        }
    }

    private synchronized void killProcess() {
        if (child != null && child.isAlive()) {
            LOG.info("Force kill {} process", mlContext.getIdentity());
            child.destroyForcibly();
            child = null;
        }
    }

}
