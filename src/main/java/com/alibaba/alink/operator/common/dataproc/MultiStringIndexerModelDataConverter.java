package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Model data converter for {@link com.alibaba.alink.pipeline.dataproc.MultiStringIndexerModel}.
 */
public class MultiStringIndexerModelDataConverter implements
    ModelDataConverter<Tuple2<Params, Iterable<Tuple3<Integer, String, Long>>>, MultiStringIndexerModelData> {
    private static final String[] MODEL_COL_NAMES = new String[]{"column_index", "token", "token_index"};

    private static final TypeInformation[] MODEL_COL_TYPES = new TypeInformation[]{
        Types.LONG, Types.STRING, Types.LONG};

    private static final TableSchema MODEL_SCHEMA = new TableSchema(MODEL_COL_NAMES, MODEL_COL_TYPES);

    @Override
    public TableSchema getModelSchema() {
        return MODEL_SCHEMA;
    }

    @Override
    public MultiStringIndexerModelData load(List<Row> rows) {
        MultiStringIndexerModelData modelData = new MultiStringIndexerModelData();
        modelData.tokenAndIndex = new ArrayList<>();
        modelData.tokenNumber = new HashMap<>();
        for (Row row : rows) {
            long colIndex = (Long) row.getField(0);
            if (colIndex < 0L) {
                modelData.meta = Params.fromJson((String) row.getField(1));
            } else {
                int columnIndex = ((Long) row.getField(0)).intValue();
                Long tokenIndex = Long.valueOf(String.valueOf(row.getField(2)));
                modelData.tokenAndIndex.add(Tuple3.of(columnIndex, (String) row.getField(1), tokenIndex));
                modelData.tokenNumber.merge(columnIndex, 1L, Long::sum);
            }
        }

        // To ensure that every columns has token number.
        int numFields = 0;
        if (modelData.meta != null) {
            numFields = modelData.meta.get(HasSelectedCols.SELECTED_COLS).length;
        }
        for (int i = 0; i < numFields; i++) {
            modelData.tokenNumber.merge(i, 0L, Long::sum);
        }
        return modelData;
    }

    @Override
    public void save(Tuple2<Params, Iterable<Tuple3<Integer, String, Long>>> modelData, Collector<Row> collector) {
        if (modelData.f0 != null) {
            collector.collect(Row.of(-1L, modelData.f0.toJson(), null));
        }
        modelData.f1.forEach(tuple -> {
            collector.collect(Row.of(tuple.f0.longValue(), tuple.f1, tuple.f2));
        });
    }
}