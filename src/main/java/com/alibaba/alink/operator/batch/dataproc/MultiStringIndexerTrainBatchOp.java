package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.dataproc.MultiStringIndexerTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Encode several columns of strings to bigint type indices. The indices are consecutive bigint type
 * that start from 0. Non-string columns are first converted to strings and then encoded. Each columns
 * are encoded separately.
 * <p>
 * <p>Several string order type is supported, including:
 * <ol>
 *     <li>random</li>
 *     <li>frequency_asc</li>
 *     <li>frequency_desc</li>
 *     <li>alphabet_asc</li>
 *     <li>alphabet_desc</li>
 * </ol> */
public final class MultiStringIndexerTrainBatchOp
    extends BatchOperator<MultiStringIndexerTrainBatchOp>
    implements MultiStringIndexerTrainParams<MultiStringIndexerTrainBatchOp> {

    public MultiStringIndexerTrainBatchOp() {
        this(new Params());
    }

    public MultiStringIndexerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public MultiStringIndexerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        final String[] selectedColNames = getSelectedCols();
        final StringIndexerUtil.OrderType orderType = StringIndexerUtil.OrderType
            .valueOf(getStringOrderType().toUpperCase());

        final String[] selectedColSqlType = new String[selectedColNames.length];
        for (int i = 0; i < selectedColNames.length; i++) {
            selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
                TableUtil.findColType(in.getSchema(), selectedColNames[i]));
        }

        DataSet<Row> inputRows = in.select(selectedColNames).getDataSet();
        DataSet<Tuple3<Integer, String, Long>> indexedToken =
            StringIndexerUtil.indexTokens(inputRows, orderType, 0L, true);

        DataSet<Row> values = indexedToken
            .mapPartition(new RichMapPartitionFunction<Tuple3<Integer, String, Long>, Row>() {
                @Override
                public void mapPartition(Iterable<Tuple3<Integer, String, Long>> values, Collector<Row> out)
                    throws Exception {
                    Params meta = null;
                    if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                        meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedColNames)
                            .set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType);
                    }
                    new MultiStringIndexerModelDataConverter().save(Tuple2.of(meta, values), out);
                }
            })
            .name("build_model");

        this.setOutput(values, new MultiStringIndexerModelDataConverter().getModelSchema());
        return this;
    }
}

