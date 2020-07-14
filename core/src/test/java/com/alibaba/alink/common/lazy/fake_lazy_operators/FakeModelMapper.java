package com.alibaba.alink.common.lazy.fake_lazy_operators;

import com.alibaba.alink.common.mapper.ModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;

public class FakeModelMapper extends ModelMapper {

    public FakeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
    }

    @Override
    public void loadModel(List<Row> modelRows) {

    }

    @Override
    public Row map(Row row) throws Exception {
        return row;
    }

    @Override
    public TableSchema getOutputSchema() {
        return getDataSchema();
    }
}
