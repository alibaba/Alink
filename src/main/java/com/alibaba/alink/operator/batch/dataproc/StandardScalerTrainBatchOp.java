package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.common.dataproc.StandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import org.apache.flink.util.Collector;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
public class StandardScalerTrainBatchOp extends BatchOperator<StandardScalerTrainBatchOp>
    implements StandardTrainParams<StandardScalerTrainBatchOp> {

    public StandardScalerTrainBatchOp() {
        super(null);
    }

    public StandardScalerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public StandardScalerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();

        TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

        StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
        converter.selectedColNames = selectedColNames;
        converter.selectedColTypes = new TypeInformation[selectedColNames.length];

        for (int i = 0; i < selectedColNames.length; i++) {
            converter.selectedColTypes[i] = Types.DOUBLE;
        }


        DataSet<Row> rows = StatisticsHelper.summary(in, selectedColNames)
            .flatMap(new BuildStandardScalerModel(converter.selectedColNames,
                converter.selectedColTypes,
                getWithMean(),
                getWithStd()));

        this.setOutput(rows, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class BuildStandardScalerModel implements FlatMapFunction<TableSummary, Row> {
        private String[] selectedColNames;
        private TypeInformation[] selectedColTypes;
        private boolean withMean;
        private boolean withStdDevs;

        public BuildStandardScalerModel(String[] selectedColNames, TypeInformation[] selectedColTypes,
                                        boolean withMean, boolean withStdDevs) {
            this.selectedColNames = selectedColNames;
            this.selectedColTypes = selectedColTypes;
            this.withMean = withMean;
            this.withStdDevs = withStdDevs;
        }

        @Override
        public void flatMap(TableSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
                converter.selectedColNames = selectedColNames;
                converter.selectedColTypes = selectedColTypes;

                converter.save(new Tuple3<>(this.withMean, this.withStdDevs, srt), collector);
            }
        }
    }

}
