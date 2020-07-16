package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Model data converter for {@link com.alibaba.alink.pipeline.dataproc.MultiStringIndexerModel}.
 */
public class BinningModelDataConverter implements
    ModelDataConverter<Iterable<FeatureBinsCalculator>, List<FeatureBinsCalculator>> {
    private static final String[] MODEL_COL_NAMES = new String[]{"FeatureBordersJson"};

    private static final TypeInformation[] MODEL_COL_TYPES = new TypeInformation[]{Types.STRING};

    private static final TableSchema MODEL_SCHEMA = new TableSchema(MODEL_COL_NAMES, MODEL_COL_TYPES);

    @Override
    public TableSchema getModelSchema() {
        return MODEL_SCHEMA;
    }

    @Override
    public List<FeatureBinsCalculator> load(List<Row> rows) {
        List<FeatureBinsCalculator> list = new ArrayList<>();
        for(Row row : rows){
            list.add(FeatureBinsUtil.deSerialize((String)row.getField(0))[0]);
        }
        return list;
    }

    @Override
    public void save(Iterable<FeatureBinsCalculator> modelData, Collector<Row> collector) {
        modelData.forEach(featureBorder -> collector.collect(Row.of(FeatureBinsUtil.serialize(featureBorder))));
    }
}