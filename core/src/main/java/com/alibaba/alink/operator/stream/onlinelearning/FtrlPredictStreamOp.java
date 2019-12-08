package com.alibaba.alink.operator.stream.onlinelearning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.FtrlPredictParams;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Ftrl predictor receive two stream : model stream and data stream. It using updated model by model stream real-time,
 * and using the newest model predict data stream.
 */
public final class FtrlPredictStreamOp extends StreamOperator<FtrlPredictStreamOp>
    implements FtrlPredictParams<FtrlPredictStreamOp> {

    private DataBridge dataBridge = null;

    public FtrlPredictStreamOp(BatchOperator model) {
        super(new Params());
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else {
            throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
        }
    }

    public FtrlPredictStreamOp(BatchOperator model, Params params) {
        super(params);
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else {
            throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
        }
    }

    @Override
    public FtrlPredictStreamOp linkFrom(StreamOperator<?>... inputs) {
        checkOpSize(2, inputs);

        try {
            DataStream<LinearModelData> modelstr = inputs[0].getDataStream()
                .flatMap(new RichFlatMapFunction<Row, Tuple2<Integer, Row>>() {
                    @Override
                    public void flatMap(Row row, Collector<Tuple2<Integer, Row>> out)
                        throws Exception {
                        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        for (int i = 0; i < numTasks; ++i) {
                            out.collect(Tuple2.of(i, row));
                        }
                    }
                }).partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) { return key; }
                }, 0).map(new MapFunction<Tuple2<Integer, Row>, Row>() {
                    @Override
                    public Row map(Tuple2<Integer, Row> value) throws Exception {
                        return value.f1;
                    }
                })
                .flatMap(new CollectModel());

            TypeInformation[] types = new TypeInformation[3];
            String[] names = new String[3];
            for (int i = 0; i < 3; ++i) {
                names[i] = inputs[0].getSchema().getFieldNames()[i + 2];
                types[i] = inputs[0].getSchema().getFieldTypes()[i + 2];
            }
            TableSchema modelSchema = new TableSchema(names, types);
            /* predict samples */
            DataStream<Row> prediction = inputs[1].getDataStream()
                .connect(modelstr)
                .flatMap(new PredictProcess(TableUtil.toSchemaJson(modelSchema),
                    TableUtil.toSchemaJson(inputs[1].getSchema()), this.getParams(), dataBridge));

            this.setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), prediction,
                new LinearModelMapper(modelSchema, inputs[1].getSchema(), getParams()).getOutputSchema()));

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.toString());
        }

        return this;
    }

    public static class CollectModel implements FlatMapFunction<Row, LinearModelData> {

        private Map<Long, List<Row>> buffers = new HashMap<>(0);

        @Override
        public void flatMap(Row inRow, Collector<LinearModelData> out) throws Exception {
            long id = (long)inRow.getField(0);
            Long nTab = (long)inRow.getField(1);

            Row row = new Row(inRow.getArity() - 2);

            for (int i = 0; i < row.getArity(); ++i) {
                row.setField(i, inRow.getField(i + 2));
            }

            if (buffers.containsKey(id) && buffers.get(id).size() == nTab.intValue() - 1) {
                buffers.get(id).add(row);
                LinearModelData ret = new LinearModelDataConverter().load(buffers.get(id));
                buffers.get(id).clear();
                System.out.println("collect model : " + id);
                out.collect(ret);
            } else {
                if (buffers.containsKey(id)) {
                    buffers.get(id).add(row);
                } else {
                    List<Row> buffer = new ArrayList<>(0);
                    buffer.add(row);
                    buffers.put(id, buffer);
                }
            }
        }
    }

    public static class PredictProcess extends RichCoFlatMapFunction<Row, LinearModelData, Row> {

        private LinearModelMapper predictor = null;
        private String modelSchemaJson;
        private String dataSchemaJson;
        private Params params;
        private int iter = 0;
        private DataBridge dataBridge;

        public PredictProcess(String modelSchemaJson, String dataSchemaJson, Params params, DataBridge dataBridge) {
            this.dataBridge = dataBridge;
            this.modelSchemaJson = modelSchemaJson;
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            this.predictor = new LinearModelMapper(TableUtil.fromSchemaJson(modelSchemaJson),
                TableUtil.fromSchemaJson(dataSchemaJson), this.params);
            if (dataBridge != null) {
                // read init model
                List<Row> modelRows = DirectReader.directRead(dataBridge);
                LinearModelData model = new LinearModelDataConverter().load(modelRows);
                this.predictor.loadModel(model);
            }
        }

        @Override
        public void flatMap1(Row row, Collector<Row> collector) throws Exception {
            collector.collect(this.predictor.map(row));
        }

        @Override
        public void flatMap2(LinearModelData linearModel, Collector<Row> collector) throws Exception {
            this.predictor.loadModel(linearModel);
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " load model : " + iter++);
        }
    }
}
