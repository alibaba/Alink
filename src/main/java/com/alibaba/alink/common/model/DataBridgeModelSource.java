package com.alibaba.alink.common.model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;

import java.util.List;

/**
 * A {@link ModelSource} implementation that reads the model from the {@link DataBridge}.
 */
public class DataBridgeModelSource implements ModelSource {

    /**
     * The DataBridge object to read model from.
     */
    private final DataBridge modelDataBridge;

    public DataBridgeModelSource(DataBridge modelDataBridge) {
        this.modelDataBridge = modelDataBridge;
    }

    @Override
    public List<Row> getModelRows(RuntimeContext runtimeContext) {
        return DirectReader.directRead(this.modelDataBridge);
    }
}
