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

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.Map;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.flink.ml.cluster.master.meta.AMMeta;
import com.alibaba.flink.ml.cluster.master.meta.AMMetaImpl;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.node.runner.ExecutionStatus;
import com.alibaba.flink.ml.cluster.node.runner.FlinkKillException;
import com.alibaba.flink.ml.cluster.node.runner.MLRunner;
import com.alibaba.flink.ml.cluster.node.runner.ScriptRunner;
import com.alibaba.flink.ml.cluster.node.runner.ScriptRunnerFactory;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.alibaba.flink.ml.cluster.role.PsRole;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.cluster.rpc.NodeServer.AMCommand;
import com.alibaba.flink.ml.proto.MLClusterDef;
import com.alibaba.flink.ml.proto.NodeSpec;
import com.alibaba.flink.ml.tensorflow2.util.TFConstants;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.alibaba.flink.ml.util.ProtoUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This runner gets TF cluster info from Flink's broadcast variable instead of
 * the AM role. It does not interact with AM role at all. So there can be no AM role
 * when using this runner.
 */
public class DLRunner implements MLRunner, Serializable {

    public static final String IPS = "Alink:dl_ips";
    public static final String PORTS = "Alink:dl_ports";

    private static Logger LOG = LoggerFactory.getLogger(DLRunner.class);
    protected long version = 0;
    protected String localIp;
    protected NodeServer server;
    protected volatile MLContext mlContext;
    protected ScriptRunner scriptRunner;
    //the execution result of this thread
    protected ExecutionStatus resultStatus;
    protected ExecutionStatus currentResultStatus;

    protected MLClusterDef mlClusterDef;


    public DLRunner(MLContext mlContext, NodeServer server) {
        this.mlContext = mlContext;
        this.server = server;
    }

    /**
     * runner register node information to application master.
     *
     * @throws Exception
     */
    @Override
    public void registerNode() throws Exception {
        return;
    }

    public static Tuple2<BaseRole, Integer> getRoleAndIndex(
        int flinkTaskIndex, int numTfWorkers) {
        BaseRole role = flinkTaskIndex < numTfWorkers ? new WorkerRole() : new PsRole();
        int roleIndex = flinkTaskIndex < numTfWorkers ? flinkTaskIndex : flinkTaskIndex - numTfWorkers;
        return Tuple2.of(role, roleIndex);
    }

    /**
     * Get cluster information from MLContext, which origins from Flink's broadcast variable.
     */
    @Override
    public void getClusterInfo() throws InterruptedException, IOException {
        AMMeta amMeta = new AMMetaImpl(mlContext);
        final Map<String, String> mlContextProps = mlContext.getProperties();

        // cluster mode
        if (mlContextProps.containsKey(IPS) && mlContextProps.containsKey(PORTS)) {
            LOG.info("Running in cluster mode.");
            int numWorkers = Integer.parseInt(mlContextProps.get(DLConstants.NUM_WORKERS));
            String[] ips = JsonConverter.fromJson(mlContextProps.get(IPS), String[].class);
            int[] ports = JsonConverter.fromJson(mlContextProps.get(PORTS), int[].class);
            for (int i = 0; i < ips.length; i++) {
                Tuple2<BaseRole, Integer> roleAndIndex = getRoleAndIndex(i, numWorkers);
                NodeSpec nodeSpec = NodeSpec.newBuilder()
                    .setIp(ips[i])
                    .setIndex(roleAndIndex.f1)
                    .setClientPort(0)
                    .setRoleName(roleAndIndex.f0.name())
                    .putProps(TFConstants.TF_PORT, String.valueOf(ports[i]))
                    .build();
                amMeta.saveNodeSpec(nodeSpec);
            }
            mlClusterDef = amMeta.restoreClusterDef();
        }
        // standalone mode: each python processes are isolated
        else {
            LOG.info("Running in standalone mode.");
            ServerSocket serverSocket = IpHostUtil.getFreeSocket();
            NodeSpec nodeSpec = NodeSpec.newBuilder()
                .setIp(localIp)
                .setIndex(mlContext.getIndex())
                .setClientPort(server.getPort())
                .setRoleName(mlContext.getRoleName())
                .putProps(TFConstants.TF_PORT, String.valueOf(serverSocket.getLocalPort()))
                .build();
            serverSocket.close();
            amMeta.saveNodeSpec(nodeSpec);
            mlClusterDef = amMeta.restoreClusterDef();
        }
    }

    protected void checkEnd() throws MLException {
        if (resultStatus == ExecutionStatus.KILLED_BY_FLINK) {
            throw new FlinkKillException("Exit per request.");
        }
    }

    @Override
    public void run() {
        resultStatus = ExecutionStatus.RUNNING;
        currentResultStatus = ExecutionStatus.RUNNING;
        try {
            // get ip
            localIp = IpHostUtil.getIpAddress();

            // get cluster
            getClusterInfo();
            Preconditions.checkNotNull(mlClusterDef);
            checkEnd();

            // set machine learning context: the cluster spec, local server ip port
            resetMLContext();
            checkEnd();

            // run python script
            runScript();
            checkEnd();
            LOG.info("run script.");
            currentResultStatus = ExecutionStatus.SUCCEED;
        } catch (Throwable e) {
            if (e instanceof FlinkKillException || e instanceof InterruptedException) {
                LOG.info("{} killed by flink.", mlContext.getIdentity());
                currentResultStatus = ExecutionStatus.KILLED_BY_FLINK;
            } else {
                //no one ask for this thread to stop, thus there must be some error occurs
                LOG.error("Got exception during python running", e);
                mlContext.addFailNum();
                currentResultStatus = ExecutionStatus.FAILED;
            }
        } finally {
            stopExecution(currentResultStatus == ExecutionStatus.SUCCEED);
            // set resultStatus value after node notified to am.
            resultStatus = currentResultStatus;
            server.setAmCommand(AMCommand.STOP);
        }
    }

    @Override
    public void runScript() throws Exception {
        mlContext.getProperties().put("script_runner_class", ProcessPythonRunnerV2.class.getCanonicalName());
        scriptRunner = ScriptRunnerFactory.getScriptRunner(mlContext);
        scriptRunner.runScript();
    }

    @Override
    public void resetMLContext() {
        String clusterStr = ProtoUtil.protoToJson(mlClusterDef);
        LOG.info("java cluster:" + clusterStr);
        System.out.println("java cluster:" + clusterStr);
        System.out.println("node server port:" + server.getPort());
        mlContext.getProperties().put(MLConstants.CONFIG_CLUSTER_PATH, clusterStr);
        mlContext.setNodeServerIP(localIp);
        mlContext.setNodeServerPort(server.getPort());
    }

    @Override
    public void startHeartBeat() throws Exception {
    }

    @Override
    public void getCurrentJobVersion() {
    }

    @Override
    public void initAMClient() throws Exception {
    }

    @Override
    public void waitClusterRunning() throws InterruptedException, MLException {
    }

    protected void stopExecution(boolean success) {
        if (scriptRunner != null) {
            IOUtils.closeQuietly(scriptRunner);
            scriptRunner = null;
        }
        if (!success) {
            mlContext.reset();
        }
    }


    @Override
    public ExecutionStatus getResultStatus() {
        return resultStatus;
    }

    //called by other thread
    @Override
    public void notifyStop() {
        if (scriptRunner != null) {
            scriptRunner.notifyKillSignal();
        }
        resultStatus = ExecutionStatus.KILLED_BY_FLINK;
    }
}

