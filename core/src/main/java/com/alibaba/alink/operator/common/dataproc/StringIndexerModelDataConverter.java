package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.ModelDataConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class StringIndexerModelDataConverter implements
    ModelDataConverter<Iterable<Tuple3<Integer, String, Long>>, List<Tuple2<String, Long>>> {

    private static final String[] MODEL_COL_NAMES = new String[]{"token", "token_index"};
    private static final TypeInformation[] MODEL_COL_TYPES = new TypeInformation[]{Types.STRING, Types.LONG};

    @Override
    public TableSchema getModelSchema() {
        return new TableSchema(MODEL_COL_NAMES, MODEL_COL_TYPES);
    }

    @Override
    public List<Tuple2<String, Long>> load(List<Row> rows) {
        List<Tuple2<String, Long>> modelData = new ArrayList<>(rows.size());
        for (Row row : rows) {
            modelData.add(Tuple2.of((String) row.getField(0), (Long) row.getField(1)));
        }
        return modelData;
    }

    @Override
    public void save(Iterable<Tuple3<Integer, String, Long>> modelData, Collector<Row> collector) {
        modelData.forEach(record -> {
            collector.collect(Row.of(record.f1, record.f2));
        });
    }
}