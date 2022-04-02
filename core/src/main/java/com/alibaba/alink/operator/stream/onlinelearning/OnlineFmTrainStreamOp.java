package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LogitLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LossFunction;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.SquareLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.OnlineFmTrainParams;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FM algorithm receive train data streams, using training samples to update model element by element, and output
 * model after every time interval.
 */

@Internal
@NameCn("在线FM训练")
public final class OnlineFmTrainStreamOp extends StreamOperator<OnlineFmTrainStreamOp>
        implements OnlineFmTrainParams<OnlineFmTrainStreamOp> {

    private static final long serialVersionUID = -1717242899554835631L;
    DataBridge dataBridge = null;

    public OnlineFmTrainStreamOp(BatchOperator <?>model) {
        super(new Params());
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
            System.out.println("FM algo: initial model is null. random generate initial model is used.");
        }
    }

    public OnlineFmTrainStreamOp(BatchOperator <?>model, Params params) {
        super(params);
        if (model != null) {
            dataBridge = DirectReader.collect(model);
        } else if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
            System.out.println("FM algo: initial model is null. random generate initial model is used.");
        }
    }

    private static int[] getSplitInfo(int coefSize, int parallelism) {
        assert (coefSize > parallelism);
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
    public OnlineFmTrainStreamOp linkFrom(StreamOperator<?>... inputs) {
        checkOpSize(1, inputs);
        int vectorSize = getVectorSize();
        int vectorTrainIdx = getVectorCol() != null ?
                TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), getVectorCol()) : -1;
        int labelIdx = TableUtil.findColIndexWithAssertAndHint(inputs[0].getColNames(), getLabelCol());

        StreamExecutionEnvironment
                streamEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment();
        int parallelism = streamEnv.getParallelism();
        final int[] splitInfo = getSplitInfo(vectorSize, parallelism);

        DataStream<Tuple3<Long, SparseVector, Object>> initData = inputs[0].getDataStream().map(
                new ParseSample(vectorTrainIdx, labelIdx));

        // train data format = Tuple3<sampleId, SparseVector(subSample), label>
        // feedback format = Tuple4<sampleId, sum_wx>
        IterativeStream.ConnectedIterativeStreams<Tuple3<Long, SparseVector, Object>,
                Tuple2<Long, Object>>
                iteration = initData.iterate(Long.MAX_VALUE)
                .withFeedbackType(TypeInformation
                        .of(new TypeHint<Tuple2<Long, Object>>() {
                        }));

        DataStream iterativeState = iteration.flatMap(new AppendWx(vectorSize))
                .flatMap(new SplitVector(splitInfo, vectorSize))
                .partitionCustom(new SubVectorPartitioner(), 1)
                .flatMap(new CalcTask(dataBridge, splitInfo, getParams()));

        DataStream<Tuple2<Long, Object>>
                iterativeStateForReduce = iterativeState.filter(
                new FilterFunction<Tuple2<Long, Object>>() {
                    private static final long serialVersionUID = -6625612851177562571L;

                    @Override
                    public boolean filter(Tuple2<Long, Object> t2) {
                        // if t2.f0 >= 0 then feedback
                        return (t2.f0 >= 0L);
                    }
                });

        DataStream<Row> output = iterativeState.filter(
                new FilterFunction<Tuple2<Long, Object>>() {
                    private static final long serialVersionUID = 8686348884274032711L;

                    @Override
                    public boolean filter(Tuple2<Long, Object> t2) {
                        /* if t2.f0 small than 0, then output */
                        return t2.f0 < 0L;
                    }
                }).flatMap(new WriteModel(getVectorSize()));

        DataStream result =
                iterativeStateForReduce.keyBy(0)
                        .flatMap(new ReduceTask())
                        .partitionCustom(new WxPartitioner(), 0);

        iteration.closeWith(result);

        TypeInformation <?>[] types = new TypeInformation[5];
        String[] names = new String[5];
        names[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_NAME;
        types[0] = ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_TYPE;
        names[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_NAME;
        types[1] = ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_TYPE;

        names[2] = "feature_id";
        types[2] = AlinkTypes.LONG;
        names[3] = "feature_weights";
        types[3] = AlinkTypes.STRING;
        names[4] = "label_type";
        types[4] = inputs[0].getColTypes()[labelIdx];

        this.setOutput(output, names, types);
        return this;
    }

    public static class ParseSample
            extends RichMapFunction<Row, Tuple3<Long, SparseVector, Object>> {
        private static final long serialVersionUID = 4275810354629537546L;
        private long counter;
        private int parallelism;
        private final int vectorTrainIdx;
        private final int labelIdx;

        public ParseSample(int vectorTrainIdx, int labelIdx) {
            this.vectorTrainIdx = vectorTrainIdx;
            this.labelIdx = labelIdx;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            counter = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public Tuple3<Long, SparseVector, Object> map(Row row) throws Exception {
            long sampleId = counter;
            counter += parallelism;
            Thread.sleep(1);
            SparseVector vec = VectorUtil.getSparseVector(row.getField(vectorTrainIdx));
            return Tuple3.of(sampleId, vec, row.getField(labelIdx));
        }
    }

    public static class AppendWx extends RichCoFlatMapFunction<Tuple3<Long, SparseVector, Object>,
            Tuple2<Long, Object>,
            Tuple4<Long, SparseVector, Object, Object>> {
        private static final long serialVersionUID = -2182229737048131290L;
        Map<Long, Tuple3<SparseVector, Object, Long>> sampleBuffer = new HashMap<>();
        private long cnt = 0L;
        private final int vectorSize;

        public AppendWx(int vectorSize) {
            this.vectorSize = vectorSize;
        }

        @Override
        public void flatMap1(Tuple3<Long, SparseVector, Object> value,
                             Collector<Tuple4<Long, SparseVector, Object, Object>> out) {
            if (cnt > 0 && (value.f0 / getRuntimeContext().getNumberOfParallelSubtasks()) % cnt != 0) {
                return;
            }
            sampleBuffer.put(value.f0, Tuple3.of(value.f1, value.f2, System.currentTimeMillis()));
            out.collect(Tuple4.of(value.f0, value.f1, value.f2, 0.0));
        }

        @Override
        public void flatMap2(Tuple2<Long, Object> value,
                             Collector<Tuple4<Long, SparseVector, Object, Object>> out)
                throws Exception {
            if (value.f0 < 0L) {
                Thread.sleep(vectorSize / 200);
                return;
            }

            Tuple3<SparseVector, Object, Long> sample = sampleBuffer.get(value.f0);
            long timeInterval = System.currentTimeMillis() - sample.f2;
            if (timeInterval > 500L) {
                if (cnt != 2L) {
                    cnt = 2L;
                    //50% samples are abandoned. current sampleId;
                }
            } else {
                if (cnt != 0L) {
                    cnt = 0L;
                    //no sample is abandoned. current sampleId
                }
            }
            out.collect(Tuple4.of(-(value.f0 + 1), sample.f0, sample.f1, value.f1));
        }
    }

    public static class SplitVector
            extends RichFlatMapFunction<Tuple4<Long, SparseVector, Object, Object>,
            Tuple6<Long, Integer, Integer, SparseVector, Object, Object>> {
        private static final long serialVersionUID = 3348048191649288404L;
        private int coefSize;
        private final int vectorSize;
        private final int[] splitInfo;
        private final int[] nnz;
        private boolean sendTimestamps = true;

        public SplitVector(int[] splitInfo, int vectorSize) {
            this.vectorSize = vectorSize;

            this.splitInfo = splitInfo;
            this.nnz = new int[splitInfo.length - 1];
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            coefSize = vectorSize;
        }

        @Override
        public void flatMap(Tuple4<Long, SparseVector, Object, Object> t4,
                            Collector<Tuple6<Long, Integer, Integer, SparseVector, Object, Object>> collector)
                throws Exception {

            if (sendTimestamps && getRuntimeContext().getIndexOfThisSubtask() == 0) {
                long time = System.currentTimeMillis();
                for (int i = 0; i < getRuntimeContext().getNumberOfParallelSubtasks(); ++i) {
                    collector.collect(Tuple6.of(time, i, -1, new SparseVector(), -1, -1));
                }
                sendTimestamps = false;
            }

            SparseVector vec = t4.f1;

            int[] indices = vec.getIndices();
            double[] values = vec.getValues();
            int pos = 1;
            int subNum = 0;
            boolean hasElement = false;
            for (int i = 0; i < indices.length; ++i) {
                if (indices[i] < splitInfo[pos]) {
                    nnz[pos - 1]++;
                    hasElement = true;
                } else {
                    pos++;
                    i--;
                    if (hasElement) {
                        subNum++;
                        hasElement = false;
                    }
                }
            }
            if (nnz[pos - 1] != 0) {
                subNum++;
            }
            pos = 0;
            for (int i = 0; i < nnz.length; ++i) {
                if (nnz[i] != 0) {
                    int[] tmpIndices = new int[nnz[i]];
                    double[] tmpValues = new double[nnz[i]];
                    System.arraycopy(indices, pos, tmpIndices, 0, nnz[i]);
                    System.arraycopy(values, pos, tmpValues, 0, nnz[i]);
                    SparseVector tmpVec = new SparseVector(coefSize, tmpIndices, tmpValues);
                    collector.collect(Tuple6.of(t4.f0, i, subNum, tmpVec, t4.f2, t4.f3));
                    pos += nnz[i];
                    nnz[i] = 0;
                }
            }
        }
    }

    public static class WriteModel
            extends RichFlatMapFunction<Tuple2<Long, Object>, Row> {
        private static final long serialVersionUID = 3487644568763785149L;
        private final int vectorSize;
        private long modelId = Long.MAX_VALUE;
        public WriteModel(int vectorSize) {
            this.vectorSize = vectorSize;
        }

        @Override
        public void flatMap(Tuple2<Long, Object> value,
                            Collector<Row> out)
                throws Exception {
            long nTable = vectorSize + 2L;
            Timestamp curModelTimestamp = new Timestamp(-value.f0);
            if (getRuntimeContext().getIndexOfThisSubtask() == 0 && (modelId != value.f0)) {
                out.collect(
                        Row.of(curModelTimestamp, nTable,
                                -1L, JsonConverter.toJson(new double[]{0.0}), null));
                modelId = value.f0;
            }

            Tuple3<Integer, Boolean, Object> model = (Tuple3<Integer, Boolean, Object>) value.f1;
            if (model.f0 == -1 && model.f2 instanceof String) {
                out.collect(Row.of(curModelTimestamp, nTable, null, model.f2, null));
            } else {
                out.collect(Row.of(curModelTimestamp, nTable, (model.f0).longValue(), JsonConverter.toJson(model.f2), null));
            }
        }
    }

    public static class CalcTask
            extends RichFlatMapFunction<Tuple6<Long, Integer, Integer, SparseVector, Object, Object>,
            Tuple2<Long, Object>> implements CheckpointedFunction {
        private static final long serialVersionUID = -8110740731083284505L;
        private final DataBridge dataBridge;
        transient private double[][] factors;
        transient private boolean[] factorState;
        transient private double[] wx;

        double[] regular;
        double learnRate;
        int[] dim;
        private int k;
        private Task task;
        private Object[] labelValues = null;

        private int startIdx;
        private final int[] poses;
        private long startTime;
        int modelSaveTimeInterval;
        private final int vectorSize;
        transient private LossFunction lossFunc = null;
        private double[] grad;
        transient private double[][] nParams;
        private final OptimMethod method;
        // model variable
        Params meta;
        private transient ListState<double[][]> modelState;
        private boolean isRestartFromCheckPoint = false;

        public CalcTask(DataBridge dataBridge, int[] poses, Params params) {
            this.dataBridge = dataBridge;
            this.poses = poses;
            this.modelSaveTimeInterval = params.get(OnlineFmTrainParams.TIME_INTERVAL) * 1000;
            learnRate = params.get(OnlineFmTrainParams.LEARN_RATE);
            method = params.get(OnlineFmTrainParams.OPTIM_METHOD);
            this.regular = new double[3];
            regular[0] = params.get(OnlineFmTrainParams.LAMBDA_0);
            regular[1] = params.get(OnlineFmTrainParams.LAMBDA_1);
            regular[2] = params.get(OnlineFmTrainParams.LAMBDA_2);
            this.vectorSize = params.get(OnlineFmTrainParams.VECTOR_SIZE);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int numWorkers = getRuntimeContext().getNumberOfParallelSubtasks();
            int workerId = getRuntimeContext().getIndexOfThisSubtask();
            startTime = Long.MAX_VALUE;
            startIdx = poses[workerId];
            int endIdx = poses[workerId + 1];
            if (!isRestartFromCheckPoint) {
                // read init model
                if (dataBridge != null) {
                    List<Row> modelRows = DirectReader.directRead(dataBridge);
                    for (Row row : modelRows) {
                        Object featureId = row.getField(0);
                        if (featureId == null) {
                            String metaParamsStr = (String) row.getField(1);
                            meta = Params.fromJson(metaParamsStr);
                            this.dim = meta.get(ModelParamName.DIM);
                            this.k = dim[2];
                            this.task = meta.get(ModelParamName.TASK);
                            this.labelValues = meta.get(ModelParamName.LABEL_VALUES);
                            meta.set(ModelParamName.VECTOR_SIZE, vectorSize);
                            break;
                        }
                    }

                    int localSize = vectorSize / numWorkers;
                    localSize += (workerId < vectorSize % numWorkers) ? 1 : 0;
                    factors = new double[localSize][k + 1];
                    factorState = new boolean[localSize];
                    Arrays.fill(factorState, false);
                    if (method.equals(OptimMethod.ADAGRAD) || method.equals(OptimMethod.RMS)) {
                        nParams = new double[localSize][k + 1];
                        fillValue(nParams);
                    }

                    for (Row row : modelRows) {
                        Object obj = row.getField(0);
                        if (obj != null) {
                            long featureId = (long) obj;
                            if (featureId >= 0L) {
                                int fid = (int) featureId;
                                if (fid >= startIdx && fid < endIdx) {
                                    factors[fid - startIdx]
                                            = JsonConverter.fromJson((String) row.getField(1), double[].class);
                                }
                            }
                        }
                    }

                } else {
                    throw new RuntimeException(
                            "FM algo: initial model is null. random generate initial model is used.");
                }
            }
            // wx[0] -- wx[dim[2]-1] is sum_vx
            // wx[dim[2]] -- wx[2*dim[2]-1] is sum_v2x2
            // wx[2*dim[2]] is linear item
            wx = new double[2 * k + 1];
            grad = new double[k + 1];

            if (task.equals(Task.REGRESSION)) {
                double minTarget = 1.0e20;
                double maxTarget = -1.0e20;

                double d = maxTarget - minTarget;
                d = Math.max(d, 1.0);
                maxTarget = maxTarget + d * 0.2;
                minTarget = minTarget - d * 0.2;

                lossFunc = new SquareLoss(maxTarget, minTarget);
            } else if (task.equals(Task.BINARY_CLASSIFICATION)) {
                lossFunc = new LogitLoss();
            } else {
                throw new RuntimeException("Invalid task: " + task);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            modelState.clear();
            modelState.add(factors);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<double[][]> descriptor =
                    new ListStateDescriptor("StreamingOnlineFtrlModelState",
                            TypeInformation.of(new TypeHint<double[][]>() {
                            }));

            modelState = context.getOperatorStateStore().getOperatorState(descriptor);
            isRestartFromCheckPoint = context.isRestored();
            if (isRestartFromCheckPoint) {
                for (double[][] state : modelState.get()) {
                    this.factors = state;
                    if (method.equals(OptimMethod.ADAGRAD) || method.equals(OptimMethod.RMS)) {
                        nParams = new double[factors.length][k + 1];
                        fillValue(nParams);
                    }
                }
            }
        }

        @Override
        public void flatMap(
                Tuple6<Long, Integer, Integer, SparseVector, Object, Object> value,
                Collector<Tuple2<Long, Object>> out) throws Exception {
            if (startTime == Long.MAX_VALUE) {
                if (value.f2 == -1) {
                    startTime = value.f0;
                    return;
                }
            }
            // save model
            if (System.currentTimeMillis() - startTime > modelSaveTimeInterval) {
                Arrays.fill(factorState, true);
                meta.set("lastModelVersion", startTime);
                startTime += modelSaveTimeInterval;
                meta.set("modelVersion", startTime);

                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                if (taskId == 0) {
                    out.collect(Tuple2.of(-startTime, Tuple3.of(-1, false, meta.toJson())));
                }

                for (int i = poses[taskId]; i < poses[taskId + 1]; ++i) {
                    out.collect(Tuple2.of(-startTime, Tuple3.of(i, factorState[i - poses[taskId]], factors[i - poses[taskId]])));
                }
                Arrays.fill(factorState, false);
                out.collect(Tuple2.of(Long.MAX_VALUE, taskId + getRuntimeContext().getNumberOfParallelSubtasks()));
            }

            if (value.f0 >= 0L) {
                Arrays.fill(wx, 0.0F);
                SparseVector vec = value.f3;

                int[] indices = vec.getIndices();
                double[] values = vec.getValues();

                for (int i = 0; i < indices.length; i++) {
                    long featurePos = indices[i];
                    double x = values[i];

                    int localIdx = (int) (featurePos - startIdx);
                    double[] w = factors[localIdx];
                    factorState[localIdx] = true;

                    // the linear term
                    if (dim[1] > 0) {
                        wx[2 * k] += x * w[k]; // w's last pos is reserved for linear weights
                    }
                    // the quadratic term
                    for (int j = 0; j < k; j++) {
                        double vixi = x * w[j];
                        wx[j] += vixi;
                        wx[k + j] += vixi * vixi;
                    }
                }
                out.collect(Tuple2.of(value.f0, Tuple2.of(value.f2, wx)));
            } else {
                double[] recvWx = (double[]) value.f5;

                // long timeInterval = System.currentTimeMillis() - t3.f2;
                SparseVector vec = value.f3;
                double y = recvWx[k];
                double label = task.equals(Task.REGRESSION) ? Double.parseDouble(value.f4.toString())
                        : (value.f4.equals(labelValues[0]) ? 1.0 : 0.0);

                // (2) compute gradient
                double dLdy = lossFunc.dldy(label, y);
                int[] indices = vec.getIndices();
                double[] values = vec.getValues();
                for (int i = 0; i < indices.length; i++) {
                    long featurePos = indices[i];
                    double x = values[i];
                    int localIdx = (int) (featurePos - startIdx);
                    double[] w = factors[localIdx];
                    factorState[localIdx] = true;

                    // the linear term
                    if (dim[1] > 0) {
                        grad[k] = dLdy * x;
                        grad[k] += regular[1] * w[k]; // regularization
                    }
                    // the quadratic term
                    for (int j = 0; j < k; j++) {
                        double vixi = x * w[j];
                        double d = x * (recvWx[j] - vixi);
                        grad[j] = dLdy * d;
                        grad[j] += regular[2] * w[j]; // regularization
                    }

                    // apply the gradient in online way
                    double[] nParam;

                    switch (method) {
                        case OSGD:
                            if (dim[1] > 0) {
                                w[k] -= learnRate * grad[k];
                            }
                            for (int j = 0; j < k; j++) {
                                w[j] -= learnRate * grad[j];
                            }
                            break;
                        case RMS:
                            nParam = nParams[localIdx];
                            for (int j = 0; j < k; j++) {
                                nParam[j] = 1.0 + 0.999 * nParam[j] + 0.1 * grad[j] * grad[j];
                            }
                            if (dim[1] > 0) {
                                w[k] -= learnRate * grad[k] / Math.sqrt(nParam[k]);
                            }
                            for (int j = 0; j < k; j++) {
                                w[j] -= learnRate * grad[j] / Math.sqrt(nParam[j]);
                            }
                            break;
                        case ADAGRAD:
                            nParam = nParams[localIdx];
                            for (int j = 0; j < k; j++) {
                                nParam[j] = nParam[j] + grad[j] * grad[j];
                            }
                            if (dim[1] > 0) {
                                w[k] -= learnRate * grad[k] / Math.sqrt(nParam[k]);
                            }
                            for (int j = 0; j < k; j++) {
                                w[j] -= learnRate * grad[j] / Math.sqrt(nParam[j]);
                            }
                            break;
                        default:
                            throw new RuntimeException("optimize method not support yet.");
                    }
                }
            }
        }
    }

    public static class ReduceTask extends
            RichFlatMapFunction<Tuple2<Long, Object>, Tuple2<Long, Object>> {

        private static final long serialVersionUID = 8796024400875966682L;
        private Map<Long, Tuple2<Integer, double[]>> buffer;

        public ReduceTask() {
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            buffer = new HashMap<>(0);
        }

        @Override
        public void flatMap(Tuple2<Long, Object> value,
                            Collector<Tuple2<Long, Object>> out)
                throws Exception {
            if (value.f0 == Long.MAX_VALUE) {
                out.collect(Tuple2.of((long) -(int) value.f1, false));
                return;
            }
            Tuple2<Integer, double[]> val = buffer.get(value.f0);
            Tuple2<Integer, double[]> t2 = (Tuple2<Integer, double[]>) value.f1;
            if (val == null) {
                val = Tuple2.of(1, t2.f1);
                buffer.put(value.f0, val);
            } else {
                val.f0++;
                for (int i = 0; i < val.f1.length; ++i) {
                    val.f1[i] += t2.f1[i];
                }
            }

            if (val.f0.equals(t2.f0)) {
                int k = val.f1.length / 2;
                double[] ret = new double[k + 1];
                ret[k] = val.f1[2 * k];
                for (int i = 0; i < k; ++i) {
                    ret[k] += 0.5 * (val.f1[i] * val.f1[i] - val.f1[k + i]);
                }
                System.arraycopy(val.f1, 0, ret, 0, k);
                out.collect(Tuple2.of(value.f0, ret));
                buffer.remove(value.f0);
            }
        }
    }

    private static class SubVectorPartitioner implements Partitioner<Integer> {
        private static final long serialVersionUID = -2937754666786979106L;

        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    private static class WxPartitioner implements Partitioner<Long> {

        private static final long serialVersionUID = -412790528707578880L;

        @Override
        public int partition(Long sampleId, int numPartition) {
            if (sampleId < 0L) {
                return (int) (-(sampleId + 1) % numPartition);
            } else {
                return (int) (sampleId % numPartition);
            }
        }
    }

    private static void fillValue(double[][] array) {
        if (array != null) {
            for (int i = 0; i < array.length; ++i) {
                for (int j = 0; j < array[0].length; ++j) {
                    array[i][j] = 1.0;
                }
            }
        }
    }
}
