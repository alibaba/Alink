package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextPredictModelData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class NaiveBayesTextModelInfoBatchOp extends
    ExtractModelInfoBatchOp<NaiveBayesTextModelInfo, NaiveBayesTextModelInfoBatchOp> {

    @Override
    protected BatchOperator<?> processModel() {
        NaiveBayesTextPredictModelData modelData = new NaiveBayesTextModelDataConverter().load(this.collect());
        NaiveBayesTextModelInfo modelInfo = new NaiveBayesTextModelInfo(modelData.modelArray, modelData.vectorSize,
            modelData.vectorColName, modelData.modelType.toString());
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of(JsonConverter.toJson(modelInfo)));
        DataSet<Row> summaryRow = MLEnvironmentFactory.get(this.getMLEnvironmentId())
            .getExecutionEnvironment().fromCollection(rows, new RowTypeInfo(Types.STRING));
        Table summaryTable = DataSetConversionUtil.toTable(this.getMLEnvironmentId(), summaryRow,
            new String[]{"modelInfo"}, new TypeInformation[]{Types.STRING});
        return new TableSourceBatchOp(summaryTable);
    }

    public NaiveBayesTextModelInfoBatchOp() {
        this(null);
    }

    public NaiveBayesTextModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected NaiveBayesTextModelInfo createModelInfo(List<Row> rows) {
        return new NaiveBayesTextModelInfo(rows);
    }
}
