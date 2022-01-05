package com.alibaba.alink.operator.common.prophet;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * the way to build the prophet model
 */
public class BuildProphetModel extends RichMapPartitionFunction<Row, Row> {
    private static final long serialVersionUID = -4019434236075549258L;
    private String[] selectedColNames;

    public BuildProphetModel(String[] selectedColNames) {
        this.selectedColNames = selectedColNames;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void mapPartition(Iterable<Row> iterable, Collector<Row> collector) throws Exception {
        StringBuilder sbd = new StringBuilder();
        Iterator<Row> iter =  iterable.iterator();
        while(iter.hasNext()) {
            Row row = iter.next();
            sbd.append(row.getField(0)).append(",");
            sbd.append(row.getField(1)).append(";");
        }
        ListenerApplication.open();
        ListenerApplication.modelParam = sbd.toString().substring(0, sbd.length() - 1);
        Runtime rt = Runtime.getRuntime();
        Process process = rt.exec("python " + BuildProphetModel.class.getClassLoader().getResource("python_file/prophet/prophet_demo.py").getPath());
        process.waitFor();
        ListenerApplication.close();
        Row row = new Row(1);
        row.setField(0, ListenerApplication.returnValue);
        collector.collect(row);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
