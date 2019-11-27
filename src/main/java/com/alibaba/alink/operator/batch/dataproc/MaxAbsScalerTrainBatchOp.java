package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import org.apache.flink.util.Collector;

/**
 * MaxAbsScaler transforms a dataSet of rows, rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsScalerTrain will train a model.
 */
public class MaxAbsScalerTrainBatchOp extends BatchOperator<MaxAbsScalerTrainBatchOp>
    implements MaxAbsScalerTrainParams<MaxAbsScalerTrainBatchOp> {

    public MaxAbsScalerTrainBatchOp() {
        super(null);
    }

    public MaxAbsScalerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public MaxAbsScalerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();

        TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

        MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
        converter.selectedColNames = selectedColNames;
        converter.selectedColTypes = new TypeInformation[selectedColNames.length];

        for (int i = 0; i < selectedColNames.length; i++) {
            converter.selectedColTypes[i] = Types.DOUBLE;
        }


        DataSet<Row> rows = StatisticsHelper.summary(in, selectedColNames)
            .flatMap(new BuildMaxAbsScalerModel(converter.selectedColNames, converter.selectedColTypes));

        this.setOutput(rows, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class BuildMaxAbsScalerModel implements FlatMapFunction<TableSummary, Row> {
        private String[] selectedColNames;
        private TypeInformation[] selectedColTypes;

        public BuildMaxAbsScalerModel(String[] selectedColNames, TypeInformation[] selectedColTypes) {
            this.selectedColNames = selectedColNames;
            this.selectedColTypes = selectedColTypes;
        }

        @Override
        public void flatMap(TableSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
                converter.selectedColNames = selectedColNames;
                converter.selectedColTypes = selectedColTypes;

                converter.save(srt, collector);
            }
        }
    }

}
