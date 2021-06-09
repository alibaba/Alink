package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.model.ModelDataConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

public class SwingRecommModelConverter
    implements ModelDataConverter<Iterable <Row>, Iterable <Row>> {
    private TypeInformation itemType;

    public SwingRecommModelConverter(TypeInformation itemType) {
        this.itemType = itemType;
    }

    @Override
    public void save(Iterable<Row> modelData, Collector<Row> collector) {
        modelData.forEach(collector::collect);
    }

    @Override
    public Iterable<Row> load(List<Row> rows) {
        return rows;
    }

    @Override
    public TableSchema getModelSchema() {
        return new TableSchema(new String[]{"mainItems", "recommItemAndSimilarity"},
            new TypeInformation[]{itemType, Types.STRING});
    }
}
