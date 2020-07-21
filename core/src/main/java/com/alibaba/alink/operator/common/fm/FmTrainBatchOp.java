package com.alibaba.alink.operator.common.fm;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.common.optim.FmOptimizer;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

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
     * @param session   environment.
     * @return
     */
    @Override
    protected DataSet<Tuple2<FmDataFormat, double[]>> optimize(DataSet<Tuple3<Double, Double, Vector>> trainData,
                                                               DataSet<Integer> vecSize,
                                                               final Params params,
                                                               final int[] dim,
                                                               MLEnvironment session) {

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
}


