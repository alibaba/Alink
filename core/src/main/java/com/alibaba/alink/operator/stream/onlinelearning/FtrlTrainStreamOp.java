package com.alibaba.alink.operator.stream.onlinelearning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LabelTypeEnum;
import com.alibaba.alink.operator.common.linear.LabelTypeEnum.StringTypeEnum;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.FtrlTrainParams;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Ftrl algorithm receive train data streams, using the training samples to update model element by element, and output
 * model after every time interval.
 */
public final class FtrlTrainStreamOp extends StreamOperator<FtrlTrainStreamOp>
    implements FtrlTrainParams<FtrlTrainStreamOp> {

    private DataBridge dataBridge = null;

    public FtrlTrainStreamOp(BatchOperator model) throws Exception {
        super(new Params());
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else {
            throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
        }
    }

    public FtrlTrainStreamOp(BatchOperator model, Params params) throws Exception {
        super(params);
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else {
            throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
        }
    }

    private static int[] getSplitInfo(int featureSize, boolean hasInterceptItem, int parallelism) {
        assert (featureSize > parallelism);
        int coefSize = (hasInterceptItem) ? featureSize + 1 : featureSize;
        int subSize = coefSize / parallelism;
        int[] poses = new int[parallelism + 1];
        int offset = coefSize % parallelism;
        for (int i = 0; i < offset; ++i) {
            poses[i + 1] = poses[i] + subSize + 1;
        }
        for (int i = offset; i < parallelism; ++i) {
            poses[i + 1] = poses[i] + subSize;
        }
        return poses;
    }

    @Override
    public FtrlTrainStreamOp linkFrom(StreamOperator<?>... inputs) {
        checkOpSize(1, inputs);
        int vectorSize = getVectorSize();
        boolean hasInterceptItem = getWithIntercept();
        int vectorTrainIdx = getVectorCol() != null ?
            TableUtil.findColIndex(inputs[0].getColNames(), getVectorCol()) : -1;
        int labelIdx = TableUtil.findColIndex(inputs[0].getColNames(), getLabelCol());
        String[] featureCols = getFeatureCols();
        int[] featureIdx = null;
        int featureColLength = -1;
        if (vectorTrainIdx == -1) {
            featureIdx = new int[featureCols.length];
            for (int i = 0; i < featureCols.length; ++i) {
                featureIdx[i] = TableUtil.findColIndex(inputs[0].getColNames(), featureCols[i]);
            }
            featureColLength = featureCols.length;
        }
        final TypeInformation labelType = inputs[0].getColTypes()[labelIdx];
        int parallelism = MLEnvironmentFactory.get(getMLEnvironmentId())
            .getStreamExecutionEnvironment().getParallelism();
        int featureSize = vectorTrainIdx != -1 ? vectorSize : featureColLength;
        final int[] splitInfo = getSplitInfo(featureSize, hasInterceptItem, parallelism);

        DataStream<Row> initData = inputs[0].getDataStream();

        // Tuple5<SampleId, taskId, numSubVec, SubVec, label>
        DataStream<Tuple5<Long, Integer, Integer, Vector, Object>> input
            = initData.flatMap(new SplitVector(splitInfo, hasInterceptItem, vectorSize,
            vectorTrainIdx, featureIdx, labelIdx))
            .partitionCustom(new CustomBlockPartitioner(), 1);

        // train data format = <sampleId, subSampleTaskId, subNum, SparseVector(subSample), label>
        // feedback format = Tuple7<sampleId, subSampleTaskId, subNum, SparseVector(subSample), label, wx,
        // timeStamps>
        IterativeStream.ConnectedIterativeStreams<Tuple5<Long, Integer, Integer, Vector, Object>,
            Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>>
            iteration = input.iterate(Long.MAX_VALUE)
            .withFeedbackType(TypeInformation
                .of(new TypeHint<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>>() {}));

        DataStream iterativeBody = iteration.flatMap(
            new CalcTask(dataBridge, splitInfo, getParams()))
            .keyBy(0)
            .flatMap(new ReduceTask(parallelism, splitInfo))
            .partitionCustom(new CustomBlockPartitioner(), 1);

        DataStream<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>>
            result = iterativeBody.filter(
            new FilterFunction<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>>() {
                @Override
                public boolean filter(Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> t3)
                    throws Exception {
                    // if t3.f0 > 0 && t3.f2 > 0 then feedback
                    return (t3.f0 > 0 && t3.f2 > 0);
                }
            });

        DataStream<Row> output = iterativeBody.filter(
            new FilterFunction<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>>() {
                @Override
                public boolean filter(Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> value)
                    throws Exception {
                    /* if value.f0 small than 0, then output */
                    return value.f0 < 0;
                }
            }).flatMap(new WriteModel(labelType, getVectorCol(), featureCols, hasInterceptItem));

        iteration.closeWith(result);

        TableSchema schema = new LinearModelDataConverter(labelType).getModelSchema();

        TypeInformation[] types = new TypeInformation[schema.getFieldTypes().length + 2];
        String[] names = new String[schema.getFieldTypes().length + 2];
        names[0] = "bid";
        names[1] = "ntab";
        types[0] = Types.LONG;
        types[1] = Types.LONG;
        for (int i = 0; i < schema.getFieldTypes().length; ++i) {
            types[i + 2] = schema.getFieldTypes()[i];
            names[i + 2] = schema.getFieldNames()[i];
        }

        this.setOutput(output, names, types);
        return this;
    }

    public static class SplitVector
        extends RichFlatMapFunction<Row, Tuple5<Long, Integer, Integer, Vector, Object>> {
        private long counter;
        private int coefSize;
        private boolean hasInterceptItem;
        private int vectorSize;
        private int parallelism;
        private int vectorTrainIdx;
        private int[] splitInfo;
        private int labelIdx;
        private int[] featureIdx;

        public SplitVector(int[] splitInfo, boolean hasInterceptItem, int vectorSize,
                           int vectorTrainIdx, int[] featureIdx, int labelIdx) {
            this.hasInterceptItem = hasInterceptItem;
            this.vectorSize = vectorSize;
            this.vectorTrainIdx = vectorTrainIdx;
            this.splitInfo = splitInfo;
            this.labelIdx = labelIdx;
            this.featureIdx = featureIdx;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            coefSize = (hasInterceptItem) ? vectorSize + 1 : vectorSize;
            counter = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(Row row, Collector<Tuple5<Long, Integer, Integer, Vector, Object>> collector)
            throws Exception {
            long sampleId = counter;
            counter += parallelism;
            Vector vec;
            if (vectorTrainIdx == -1) {
                vec = new DenseVector(featureIdx.length);
                for (int i = 0; i < featureIdx.length; ++i) {
                    vec.set(i, Double.valueOf(row.getField(featureIdx[i]).toString()));
                }
            } else {
                vec = VectorUtil.getVector(row.getField(vectorTrainIdx));
            }

            if (hasInterceptItem) {
                vec = vec.prefix(1.0);
            }

            if (vec instanceof SparseVector) {
                int[] indices = ((SparseVector)vec).getIndices();
                double[] values = ((SparseVector)vec).getValues();
                int pos = 0;
                int subNum = 0;
                Map<Integer, Vector> tmpVec = new HashMap<>();
                Map<Integer, Double> list = new HashMap<>();
                for (int i = 0; i < indices.length; ++i) {
                    if (indices[i] >= splitInfo[pos] && indices[i] < splitInfo[pos + 1]) {
                        list.put(indices[i], values[i]);
                        if (i == indices.length - 1) {
                            SparseVector tmp = new SparseVector(coefSize, list);
                            tmpVec.put(pos, tmp);
                            list.clear();
                            pos++;
                            subNum++;
                            break;
                        }
                    } else if (list.size() != 0) {
                        SparseVector tmp = new SparseVector(coefSize, list);
                        tmpVec.put(pos, tmp);
                        list.clear();
                        pos++;
                        subNum++;
                        i--;
                    } else {
                        pos++;
                        i--;
                    }

                }
                for (Integer key : tmpVec.keySet()) {
                    collector.collect(Tuple5.of(sampleId, key, subNum, tmpVec.get(key), row.getField(labelIdx)));
                }
            } else {
                double[] data = ((DenseVector)vec).getData();
                for (int i = 0; i < splitInfo.length - 1; ++i) {
                    DenseVector dvec = new DenseVector(splitInfo[i + 1] - splitInfo[i]);
                    for (int j = splitInfo[i]; j < splitInfo[i + 1]; ++j) {
                        dvec.set(j - splitInfo[i], data[j]);
                    }
                    collector.collect(Tuple5.of(sampleId, i, parallelism, dvec, row.getField(labelIdx)));
                }
            }
        }
    }

    public static class WriteModel
        extends RichFlatMapFunction<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>, Row> {
        private LabelTypeEnum.StringTypeEnum type;
        private long iter = 0L;
        private String vectorColName;
        private String[] featureCols;
        private boolean hasInterceptItem;

        public WriteModel(TypeInformation type, String vectorColName, String[] featureCols, boolean hasInterceptItem) {
            this.type = LabelTypeEnum.StringTypeEnum.valueOf(type.toString().toUpperCase());
            this.vectorColName = vectorColName;
            this.featureCols = featureCols;
            this.hasInterceptItem = hasInterceptItem;
        }

        @Override
        public void flatMap(Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> value, Collector<Row> out)
            throws Exception {

            LinearModelData modelData = new LinearModelData();
            modelData.coefVector = (DenseVector)value.f3;
            modelData.hasInterceptItem = this.hasInterceptItem;
            modelData.vectorColName = this.vectorColName;
            modelData.modelName = "Logistic Regression";
            modelData.featureNames = this.featureCols;
            modelData.labelValues = (Object[])value.f4;
            modelData.vectorSize = hasInterceptItem ? modelData.coefVector.size() - 1 : modelData.coefVector.size();
            modelData.linearModelType = LinearModelType.LR;

            RowCollector listCollector = new RowCollector();
            new LinearModelDataConverter().save(modelData, listCollector);
            List<Row> rows = listCollector.getRows();

            for (Row r : rows) {
                int rowSize = r.getArity();
                Row row = new Row(rowSize + 2);
                row.setField(0, iter);
                row.setField(1, (long) rows.size());

                for (int j = 0; j < rowSize; ++j) {
                    if (j == 2 && r.getField(j) != null) {
                        if (type.equals(StringTypeEnum.BIGINT) || type.equals(StringTypeEnum.LONG)) {
                            row.setField(2 + j, Double.valueOf(r.getField(j).toString()).longValue());
                        } else if (type.equals(StringTypeEnum.INT) || type.equals(StringTypeEnum.INTEGER)) {
                            row.setField(2 + j, Double.valueOf(r.getField(j).toString()).intValue());
                        } else if (type.equals(StringTypeEnum.DOUBLE)) {
                            row.setField(2 + j, Double.valueOf(r.getField(j).toString()));
                        } else if (type.equals(StringTypeEnum.FLOAT)) {
                            row.setField(2 + j, Double.valueOf(r.getField(j).toString()).floatValue());
                        } else {
                            row.setField(2 + j, r.getField(j));
                        }
                    } else {
                        row.setField(2 + j, r.getField(j));
                    }
                }
                out.collect(row);
            }

            iter++;
        }
    }

    public static class CalcTask extends RichCoFlatMapFunction<Tuple5<Long, Integer, Integer, Vector, Object>,
        Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>,
        Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>> {
        private DataBridge dataBridge;
        transient private double[] coef;
        transient private double[] nParam;
        transient private double[] zParam;

        private double alpha = 0.1;
        private double beta = 1;
        private double l1 = 1;
        private double l2 = 1;
        private int startIdx;
        private int endIdx;
        private int[] poses;
        long startTime = System.currentTimeMillis();
        int modelSaveTimeInterval;
        private Object[] labelValues;
        private boolean savedFristModel = false;
        private long modelId = 0;

        public CalcTask(DataBridge dataBridge, int[] poses, Params params) {
            this.dataBridge = dataBridge;
            this.poses = poses;
            this.alpha = params.get(FtrlTrainParams.ALPHA);
            this.beta = params.get(FtrlTrainParams.BETA);
            this.l1 = params.get(FtrlTrainParams.L_1);
            this.l2 = params.get(FtrlTrainParams.L_2);
            this.modelSaveTimeInterval = params.get(FtrlTrainParams.TIME_INTERVAL) * 1000;
            this.labelValues = null;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            int numWorkers = getRuntimeContext().getNumberOfParallelSubtasks();
            int workerId = getRuntimeContext().getIndexOfThisSubtask();
            Random random = new Random(1);

            // read init model
            List<Row> modelRows = DirectReader.directRead(dataBridge);
            LinearModelData model = new LinearModelDataConverter().load(modelRows);
            labelValues = model.labelValues;
            int weightSize = model.coefVector.size();

            if (weightSize < numWorkers) {
                throw new IllegalArgumentException("Ftrl algo: feature Size is smaller than worker num.");
            }
            int averNum = weightSize / numWorkers;
            startIdx = poses[workerId];
            endIdx = poses[workerId + 1];
            averNum += (workerId < weightSize % numWorkers) ? 1 : 0;
            coef = new double[averNum];
            nParam = new double[averNum];
            zParam = new double[averNum];

            for (int i = startIdx; i < endIdx; ++i) {
                coef[i - startIdx] = (dataBridge != null) ? model.coefVector.get(i) : random.nextDouble();
            }
            startTime = System.currentTimeMillis();
        }

        @Override
        public void flatMap1(Tuple5<Long, Integer, Integer, Vector, Object> value,
                             Collector<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>> out)
            throws Exception {
            if (!savedFristModel) {
                out.collect(Tuple7.of(-1L, 0, getRuntimeContext().getIndexOfThisSubtask(),
                    new DenseVector(coef), labelValues, -1.0, modelId++));
                savedFristModel = true;
            }
            Long timeStamps = System.currentTimeMillis();
            double wx = 0.0;
            Long sampleId = value.f0;
            Vector vec = value.f3;
            if (vec instanceof SparseVector) {
                int[] indices = ((SparseVector)vec).getIndices();

                for (int i = 0; i < indices.length; ++i) {
                    wx += ((SparseVector)vec).getValues()[i] * coef[indices[i] - startIdx];
                }
            } else {
                for (int i = 0; i < vec.size(); ++i) {
                    wx += vec.get(i) * coef[i];
                }
            }
            out.collect(Tuple7.of(sampleId, value.f1, value.f2, value.f3, value.f4, wx, timeStamps));
        }

        @Override
        public void flatMap2(Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> value,
                             Collector<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>> out)
            throws Exception {
            double p = value.f5;
            long timeInterval = System.currentTimeMillis() - value.f6;
            Vector vec = value.f3;

            /* eta */
            p = 1 / (1 + Math.exp(-p));
            double label;
            if (labelValues[0] instanceof Number) {
                label = (Double.valueOf(value.f4.toString())
                    .equals(Double.valueOf(labelValues[0].toString()))) ? 1.0 : 0.0;
            } else {
                label = (value.f4.toString().equals(labelValues[0].toString())) ? 1.0 : 0.0;
            }

            if (vec instanceof SparseVector) {
                int[] indices = ((SparseVector)vec).getIndices();
                double[] values = ((SparseVector)vec).getValues();

                for (int i = 0; i < indices.length; ++i) {
                    // update zParam nParam
                    int id = indices[i] - startIdx;
                    double g = (p - label) * values[i] / Math.sqrt(timeInterval);
                    double sigma = (Math.sqrt(nParam[id] + g * g) - Math.sqrt(nParam[id])) / alpha;
                    zParam[id] += g - sigma * coef[id];
                    nParam[id] += g * g;

                    // update model coefficient
                    if (Math.abs(zParam[id]) <= l1) {
                        coef[id] = 0.0;
                    } else {
                        coef[id] = ((zParam[id] < 0 ? -1 : 1) * l1 - zParam[id])
                            / ((beta + Math.sqrt(nParam[id]) / alpha + l2));
                    }
                }
            } else {
                double[] data = ((DenseVector)vec).getData();

                for (int i = 0; i < data.length; ++i) {
                    // update zParam nParam
                    double g = (p - label) * data[i] / Math.sqrt(timeInterval);
                    double sigma = (Math.sqrt(nParam[i] + g * g) - Math.sqrt(nParam[i])) / alpha;
                    zParam[i] += g - sigma * coef[i];
                    nParam[i] += g * g;

                    // update model coefficient
                    if (Math.abs(zParam[i]) <= l1) {
                        coef[i] = 0.0;
                    } else {
                        coef[i] = ((zParam[i] < 0 ? -1 : 1) * l1 - zParam[i])
                            / ((beta + Math.sqrt(nParam[i]) / alpha + l2));
                    }
                }
            }

            if (System.currentTimeMillis() - startTime > modelSaveTimeInterval) {
                startTime = System.currentTimeMillis();
                out.collect(Tuple7.of(-1L, 0, getRuntimeContext().getIndexOfThisSubtask(),
                    new DenseVector(coef), labelValues, -1.0, modelId++));
            }
        }
    }

    public static class ReduceTask extends
        RichFlatMapFunction<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>,
            Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>> {
        private int parallelism;
        private int[] poses;
        private Map<Long, List<Object>> buffer;

        private Map<Long, List<Tuple2<Integer, DenseVector>>> models = new HashMap<>();

        public ReduceTask(int parallelism, int[] poses) {
            this.parallelism = parallelism;
            this.poses = poses;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            buffer = new HashMap<>(0);
        }

        @Override
        public void flatMap(Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> value,
                            Collector<Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>> out)
            throws Exception {
            if (value.f0 < 0) {
                List<Tuple2<Integer, DenseVector>> model = models.get(value.f6);
                if (model == null) {
                    model = new ArrayList<>();
                    model.add(Tuple2.of(value.f2, (DenseVector)value.f3));
                    models.put(value.f6, model);
                } else {
                    model.add(Tuple2.of(value.f2, (DenseVector)value.f3));
                }
                if (model.size() == parallelism) {
                    DenseVector coef = new DenseVector(poses[parallelism]);
                    for (Tuple2<Integer, DenseVector> t2 : model) {
                        int pos = poses[t2.f0];
                        for (int i = 0; i < t2.f1.size(); ++i) {
                            coef.set(pos + i, t2.f1.get(i));
                        }
                    }
                    value.f1 = 0;
                    value.f3 = coef;
                    out.collect(value);
                    models.remove(value.f6);
                }
                return;
            }

            List<Object> val = buffer.get(value.f0);

            if (val == null) {
                val = new ArrayList<>();
                val.add(value);
                buffer.put(value.f0, val);
            } else {
                val.add(value);
            }

            if (val.size() == value.f2) {
                double y = 0.0;
                for (Object t7 : val) {
                    Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> tmp
                        = (Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>)t7;
                    y += tmp.f5;
                }

                for (int i = 0; i < value.f2; ++i) {
                    Tuple7<Long, Integer, Integer, Vector, Object, Double, Long> ret
                        = (Tuple7<Long, Integer, Integer, Vector, Object, Double, Long>)val.get(i);
                    ret.f5 = y;
                    out.collect(ret);
                }
                buffer.remove(value.f0);
            } else if (val.size() > parallelism) {
                throw new Exception("Ftrl algo : collect size > parallelism. collect size : "
                    + val.size() + ", parallelism: " + parallelism);
            }
        }
    }

    private static class CustomBlockPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }
}
