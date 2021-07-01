package com.alibaba.alink.operator.common.prophet;

import com.alibaba.alink.common.model.ModelDataConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * the prophet model data converter
 */
public class ProphetModelDataConverter implements ModelDataConverter<String, String> {

    @Override
    public void save(String s, Collector<Row> collector) {
        collector.collect(Row.of(s));
    }

    @Override
    public String load(List<Row> list) {
        if(list.size() == 1) {
            return String.valueOf(list.get(0).getField(0));
        } else {
            return null;
        }
    }

    @Override
    public TableSchema getModelSchema() {
        TableSchema tableSchema = new TableSchema(new String[]{"prophet"}, new TypeInformation[]{Types.STRING});
        return tableSchema;
    }
}
