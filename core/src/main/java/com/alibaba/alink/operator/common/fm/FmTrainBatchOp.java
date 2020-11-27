package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.common.optim.FmOptimizer;
import com.alibaba.alink.params.recommendation.FmTrainParams;

/**
 * FM model training.
 */
public class FmTrainBatchOp<T extends FmTrainBatchOp<T>> extends BaseFmTrainBatchOp<T> {

    private static final long serialVersionUID = -3985394692845121356L;

    /**
     * construct function.
     *
     * @param params parameters needed by training process.
     * @param task   Fm task: maybe "classification" or "regression".
     */
    public FmTrainBatchOp(Params params, String task) {
        super(params.set(ModelParamName.TASK, task));
    }

    /**
     * construct function.
     *
     * @param task
     */
    public FmTrainBatchOp(String task) {
        super(new Params().set(ModelParamName.TASK, task));
    }

    /**
     * optimize function.
     *
     * @param trainData training Data.
     * @param vecSize   vector size.
     * @param params    parameters.
     * @param dim       dimension.
     * @return
     */
    @Override
    protected DataSet<Tuple2<FmDataFormat, double[]>> optimize(DataSet<Tuple3<Double, Double, Vector>> trainData,
                                                               DataSet<Integer> vecSize,
                                                               final Params params,
                                                               final int[] dim) {

        final double initStdev = params.get(FmTrainParams.INIT_STDEV);

        DataSet<FmDataFormat> initFactors = vecSize.map(new RichMapFunction<Integer, FmDataFormat>() {
            private static final long serialVersionUID = 76796953320215874L;

            @Override
            public FmDataFormat map(Integer value) throws Exception {
                FmDataFormat innerModel = new FmDataFormat(value, dim, initStdev);

                return innerModel;
            }
        });

        FmOptimizer optimizer = new FmOptimizer(trainData, params);
        optimizer.setWithInitFactors(initFactors);
        return optimizer.optimize();
    }

    /**
     * The api for transforming model format.
     *
     * @param model       model with fm data format.
     * @param labelValues label values.
     * @param vecSize     vector size.
     * @param params      parameters.
     * @param dim         dimension.
     * @param isRegProc   is regression process.
     * @param labelType   label type.
     * @return model rows.
     */
    @Override
    protected DataSet<Row> transformModel(DataSet<Tuple2<FmDataFormat, double[]>> model,
                                          DataSet<Object[]> labelValues,
                                          DataSet<Integer> vecSize,
                                          Params params,
                                          int[] dim,
                                          boolean isRegProc,
                                          TypeInformation labelType) {
        return model.flatMap(new GenerateModelRows(params, dim, labelType, isRegProc))
                .withBroadcastSet(labelValues, LABEL_VALUES)
                .withBroadcastSet(vecSize, VEC_SIZE);
    }

    /**
     * generate model in row format.
     */
    public static class GenerateModelRows extends RichFlatMapFunction<Tuple2<FmDataFormat, double[]>, Row> {
        private static final long serialVersionUID = -380930181466110905L;
        private Params params;
        private int[] dim;
        private TypeInformation labelType;
        private Object[] labelValues;
        private boolean isRegProc;
        private int vecSize;

        public GenerateModelRows(Params params, int[] dim, TypeInformation labelType, boolean isRegProc) {
            this.params = params;
            this.labelType = labelType;
            this.dim = dim;
            this.isRegProc = isRegProc;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.labelValues = (Object[]) getRuntimeContext().getBroadcastVariable(LABEL_VALUES).get(0);
            this.vecSize = (int) getRuntimeContext().getBroadcastVariable(VEC_SIZE).get(0);
        }

        @Override
        public void flatMap(Tuple2<FmDataFormat, double[]> value, Collector<Row> out) throws Exception {
            FmModelData modelData = new FmModelData();
            modelData.fmModel = value.f0;
            modelData.vectorColName = params.get(FmTrainParams.VECTOR_COL);
            modelData.featureColNames = params.get(FmTrainParams.FEATURE_COLS);
            modelData.dim = dim;
            modelData.labelColName = params.get(FmTrainParams.LABEL_COL);
            modelData.task = Task.valueOf(params.get(ModelParamName.TASK).toUpperCase());
            modelData.convergenceInfo = value.f1;
            if (!isRegProc) {
                modelData.labelValues = this.labelValues;
            } else {
                modelData.labelValues = new Object[]{0.0};
            }

            modelData.vectorSize = vecSize;
            new FmModelDataConverter(labelType).save(modelData, out);
        }
    }
}


