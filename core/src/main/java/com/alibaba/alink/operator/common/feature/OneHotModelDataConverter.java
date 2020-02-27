package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class OneHotModelDataConverter implements
    ModelDataConverter<Tuple2<Params, Iterable<Tuple3<Integer, String, Long>>>, OneHotModelData> {

    private MultiStringIndexerModelDataConverter converter = new MultiStringIndexerModelDataConverter();

    @Override
    public TableSchema getModelSchema() {
        return converter.getModelSchema();
    }

    @Override
    public OneHotModelData load(List<Row> rows) {
        OneHotModelData modelData = new OneHotModelData();
        modelData.modelData = converter.load(rows);
        return modelData;
    }

    @Override
    public void save(Tuple2<Params, Iterable<Tuple3<Integer, String, Long>>> modelData, Collector<Row> collector) {
        converter.save(modelData, collector);
    }
}
