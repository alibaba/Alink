package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class NaiveBayesModelInfoBatchOp
    extends ExtractModelInfoBatchOp<NaiveBayesModelInfo, NaiveBayesModelInfoBatchOp> {

    public NaiveBayesModelInfoBatchOp() {
        this(new Params());
    }

    public NaiveBayesModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected NaiveBayesModelInfo createModelInfo(List<Row> rows) {
        return new NaiveBayesModelInfo(rows);
    }

    @Override
    protected BatchOperator<?> processModel() {
        NaiveBayesModelData modelData = new NaiveBayesModelDataConverter().load(this.collect());
        NaiveBayesModelInfo modelInfo = new NaiveBayesModelInfo(modelData.featureNames,
            modelData.isCate,
            modelData.labelWeights,
            modelData.label,
            modelData.weightSum,
            modelData.featureInfo,
            null);
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of(-10L, JsonConverter.toJson(modelInfo), null));
        rows.addAll(modelData.stringIndexerModelSerialized);
        DataSet<Row> rowModelInfo = MLEnvironmentFactory.get(this.getMLEnvironmentId())
            .getExecutionEnvironment()
            .fromCollection(rows, new RowTypeInfo(Types.LONG, Types.STRING, Types.INT));
        Table tableModelInfo = DataSetConversionUtil
            .toTable(this.getMLEnvironmentId(),
                rowModelInfo,
                new String[]{"id", "modelInfo", "val"},
                new TypeInformation[]{Types.LONG, Types.STRING, Types.INT});
        return new TableSourceBatchOp(tableModelInfo);
    }
}
