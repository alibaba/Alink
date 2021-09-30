package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.utils.DLUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.dl.utils.DataSetDiskDownloader;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * TF/Pytorch DL node, both parameter servers and workers are launched within this operator.
 */
public class DLFlatMapFunction implements Closeable, Serializable {
    private MLConfig config;
    private TypeInformation<Row> inTI;
    private TypeInformation<Row> outTI;
    private int numOutputFields;
    private MLContext mlContext;
    private FutureTask<Void> serverFuture;
    private ExecutionMode mode;
    private volatile Collector<Row> collector = null;
    private transient DataExchange<Row, Row> dataExchange;

    private static final Logger LOG = LoggerFactory.getLogger(DLFlatMapFunction.class);

    public DLFlatMapFunction(ExecutionMode mode, MLConfig config, TableSchema inputSchema,
                             TableSchema outputSchema) {
        this.mode = mode;
        this.config = config;
        this.outTI = new RowTypeInfo(inputSchema.getFieldTypes(), inputSchema.getFieldNames());
        this.outTI = new RowTypeInfo(outputSchema.getFieldTypes(), outputSchema.getFieldNames());
        this.numOutputFields = outputSchema.getFieldNames().length;
    }

    public static void prepareBroadcastData(String workDir, RuntimeContext runtimeContext,
                                            MLContext mlContext) throws Exception {
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            if (!runtimeContext.hasBroadcastVariable(DLConstants.BC_NAME_PREFIX + i)) {
                break;
            }
            List<Row> rows = runtimeContext.getBroadcastVariable(DLConstants.BC_NAME_PREFIX + i);
            // TODO: now we assumed that the bc data has only two columns: id, value.
            String fn = workDir + File.separator + "bc_data_" + i;
            try (FileWriter writer = new FileWriter(fn);
                 BufferedWriter bw = new BufferedWriter(writer)) {
                for (Row row : rows) {
                    StringBuilder sbd = new StringBuilder();
                    for (int j = 0; j < row.getArity(); j++) {
                        if (j > 0) {
                            sbd.append(" ");
                        }
                        sbd.append(row.getField(j));
                    }
                    sbd.append("\n");
                    bw.write(sbd.toString());
                }
            } catch (IOException e) {
                throw new RuntimeException("Fail to write broadcast data to local disk.");
            }
            LOG.info("Succ in writing bc data to {}", fn);
            DLUtils.safePutProperties(mlContext, DLConstants.BC_NAME_PREFIX + i, fn);
        }
    }

    /**
     * create machine learning node and data exchange object.
     *
     * @param runtimeContext flink operator RuntimeContext.
     * @throws Exception
     */
    public void open(RuntimeContext runtimeContext) throws Exception {
        int numWorkers = Integer.parseInt(this.config.getProperties().get(DLConstants.NUM_WORKERS));
        int numPSs = Integer.parseInt(this.config.getProperties().get(DLConstants.NUM_PSS));
        List<Row> bc = runtimeContext.getBroadcastVariable(DLConstants.IP_PORT_BC_NAME);
        Preconditions.checkArgument(bc.size() == (numWorkers + numPSs), "Some IPs and ports are missing.");
        List<Tuple3<Integer, String, Integer>> taskIpPorts = new ArrayList<>(bc.size());
        bc.forEach(row -> {
            String info = (String) row.getField(numOutputFields);
            String[] splited = info.split("-");
            taskIpPorts.add(Tuple3.of(Integer.parseInt(splited[0]), splited[1], Integer.parseInt(splited[2])));
        });

        int thisTaskIndex = runtimeContext.getIndexOfThisSubtask();
        Tuple2<BaseRole, Integer> roleAndIndex = DLRunner.getRoleAndIndex(thisTaskIndex, numWorkers);

        String workDir = PythonFileUtils.createTempWorkDir(String.format("temp_%d_", runtimeContext.getIndexOfThisSubtask()));
        Map <String, String> properties = config.getProperties();
        properties.put(MLConstants.WORK_DIR, workDir);
        properties.put(DLConstants.WORK_DIR, workDir);

        mlContext = new MLContext(mode, config, roleAndIndex.f0.name(), roleAndIndex.f1,
            config.getEnvPath(), null);

        // Update external files-related properties according to workDir
        {
            String pythonEnv = properties.get(DLConstants.PYTHON_ENV);
            properties.put(MLConstants.VIRTUAL_ENV_DIR, new File(workDir, pythonEnv).getAbsolutePath());

            String entryScriptFileName = PythonFileUtils.getFileName(properties.get(DLConstants.ENTRY_SCRIPT));
            mlContext.setPythonDir(new File(workDir).toPath());
            mlContext.setPythonFiles(new String[] {new File(workDir, entryScriptFileName).getAbsolutePath()});
        }

        if (runtimeContext.hasBroadcastVariable(DLConstants.BC_NAME_DOWNLOAD_PATHS)) {
            String[] downloadPathCandidates = runtimeContext.getBroadcastVariable(DLConstants.BC_NAME_DOWNLOAD_PATHS)
                .stream()
                .map(d -> (String) ((Row) d).getField(0))
                .toArray(String[]::new);
            DataSetDiskDownloader.moveFilesToWorkDir(downloadPathCandidates, new File(workDir));
        }

        if (runtimeContext.hasBroadcastVariable(DLConstants.BC_NAME_TENSOR_SHAPES)) {
            Map <String, long[]> tensorShapeMap =
                (Map <String, long[]>) (runtimeContext.getBroadcastVariable(DLConstants.BC_NAME_TENSOR_SHAPES)).get(0);
            String fn = workDir + File.separator + "tensor_shapes.txt";
            try (FileWriter writer = new FileWriter(fn);
                 BufferedWriter bw = new BufferedWriter(writer)) {
                bw.write(JsonConverter.toJson(tensorShapeMap));
            } catch (IOException e) {
                throw new RuntimeException("Fail to write tensor shape map to local disk.");
            }
            LOG.info("Succ in writing tensor shape map to {}", fn);
        }

        prepareBroadcastData(workDir, runtimeContext, mlContext);

        // put the ips and ports to mlContext so that they can be accessed by TFRunner.
        String[] ips = new String[taskIpPorts.size()];
        int[] ports = new int[taskIpPorts.size()];
        for (Tuple3<Integer, String, Integer> taskIpPort : taskIpPorts) {
            int taskId = taskIpPort.f0;
            ips[taskId] = taskIpPort.f1;
            if (thisTaskIndex == taskId) {
                Preconditions.checkArgument(ips[taskId].equals(IpHostUtil.getIpAddress()),
                    "task allocation changed");
            }
            ports[taskId] = taskIpPort.f2;
        }
        DLUtils.safePutProperties(mlContext, DLRunner.IPS, JsonConverter.toJson(ips));
        DLUtils.safePutProperties(mlContext, DLRunner.PORTS, JsonConverter.toJson(ports));

        dataExchange = new DataExchange<>(mlContext);

        try {
            serverFuture = new FutureTask<>(new NodeServer(mlContext, roleAndIndex.f0.name()), null);
            Thread t = new Thread(serverFuture);
            t.setDaemon(true);
            t.setName("NodeServer_" + mlContext.getIdentity());
            t.start();
        } catch (Exception e) {
            LOG.error("Fail to start node service.", e);
            throw new IOException(e.getMessage());
        }
        System.out.println("start:" + mlContext.getRoleName() + " index:" + mlContext.getIndex());
    }

    /**
     * stop machine learning node and resource.
     */
    @Override
    public void close() {
        /**
         * `close` is called 2 times, but `drainRead` throws SIGSEGV in the Unsafe.getLong method at the second time.
         * To avoid this, we have to exit asap.
         */
        if (null == mlContext) {
            return;
        }

        if (mlContext.getOutputQueue() != null) {
            mlContext.getOutputQueue().markFinished();
        }
        // in ps-based training, pss do not need to wait for reading.
        if(mlContext.getRoleName() == "ps"){
            LOG.info("PS job return");
            return;
        }
        // wait for tf thread finish
        try {
            //as in batch mode, we can't user timer to drain queue, so drain it here
            drainRead(collector, true);
            if (serverFuture != null && !serverFuture.isCancelled()) {
                serverFuture.get();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted waiting for server join {}.", e.getMessage());
            serverFuture.cancel(true);
        } catch (ExecutionException e) {
            LOG.error(mlContext.getIdentity() + " node server failed");
            throw new RuntimeException(e);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            serverFuture = null;
            long mumReadRecords = dataExchange.getReadRecords();
            int failNum = 0;

            if (mlContext != null) {
                failNum = mlContext.getFailNum();
                try {
                    mlContext.close();
                } catch (IOException e) {
                    LOG.error("Fail to close mlContext.", e);
                }
                mlContext = null;
            }
            if (failNum > 0) {
                //noinspection ThrowFromFinallyBlock
                throw new RuntimeException("Python script run failed, please check TaskManager logs.");
            } else {
                LOG.info("Records output: " + mumReadRecords);
            }
        }
    }

    /**
     * process input data and collect results.
     *
     * @param out output result.
     * @throws Exception
     */
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        this.collector = out;
        //put the read & write in a loop to avoid dead lock between write queue and read queue.
        boolean writeSuccess = false;
        do {
            drainRead(collector, false);
            writeSuccess = dataExchange.write(DLUtils.encodeStringValue(value));
            if (!writeSuccess) {
                Thread.yield();
            }
        } while (!writeSuccess);
    }

    public TypeInformation<Row> getProducedType() {
        return outTI;
    }

    private void drainRead(Collector<Row> out, boolean readUntilEOF) {
        while (true) {
            try {
                Object r = dataExchange.read(readUntilEOF);
                if (r != null) {
                    out.collect((Row) r);
                } else {
                    break;
                }
            } catch (InterruptedIOException iioe) {
                LOG.info("{} Reading from is interrupted, canceling the server", mlContext.getIdentity());
                serverFuture.cancel(true);
            } catch (IOException e) {
                LOG.error("Fail to read data from python.", e);
                throw new RuntimeException(e);
            }
        }
    }
}
