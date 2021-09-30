package com.alibaba.alink.common.dl;

import com.alibaba.flink.ml.util.IpHostUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.net.ServerSocket;

/**
 * flatMapFunction to collect IPs and ports.
 */
public class IpPortFlatMapFunction implements Closeable, Serializable {
    private transient RuntimeContext runtimeContext;

    private static final Logger LOG = LoggerFactory.getLogger(IpPortFlatMapFunction.class);

    public void open(RuntimeContext runtimeContext) throws Exception {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void close() {
    }

    public void flatMap(Row value, Collector<Row> out) throws Exception {
        int taskId = runtimeContext.getIndexOfThisSubtask();
        ServerSocket serverSocket = IpHostUtil.getFreeSocket();
        int port = serverSocket.getLocalPort();
        serverSocket.close();
        String localIp = IpHostUtil.getIpAddress();
        out.collect(Row.of(String.format("%d-%s-%d", taskId, localIp, port)));
    }
}
