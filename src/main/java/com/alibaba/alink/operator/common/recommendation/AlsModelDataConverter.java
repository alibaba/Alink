package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.model.ModelDataConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The ModelDataConverter for {@link com.alibaba.alink.pipeline.recommendation.ALSModel}.
 */
public class AlsModelDataConverter implements ModelDataConverter<Iterable<Tuple3<Byte, Long, float[]>>, AlsModelData> {

    private String userCol;
    private String itemCol;

    public AlsModelDataConverter(String userCol, String itemCol) {
        this.userCol = userCol;
        this.itemCol = itemCol;
    }

    public static long getVertexId(Row modelRow) {
        if (modelRow.getField(0) != null) {
            return (Long) modelRow.getField(0);
        } else {
            return (Long) modelRow.getField(1);
        }
    }

    public static boolean getIsUser(Row modelRow) {
        return modelRow.getField(0) != null;
    }

    public static float[] getFactors(Row modelRow) {
        String factorsStr = (String) modelRow.getField(2);
        String[] splited = factorsStr.split(" ");
        float[] factors = new float[splited.length];
        for (int i = 0; i < splited.length; i++) {
            factors[i] = Float.valueOf(splited[i]);
        }
        return factors;
    }

    @Override
    public TableSchema getModelSchema() {
        return new TableSchema(new String[]{userCol, itemCol, "factors"},
            new TypeInformation[]{Types.LONG, Types.LONG, Types.STRING});
    }

    @Override
    public AlsModelData load(List<Row> rows) {
        AlsModelData modelData = new AlsModelData();
        int numUsers = 0;
        int numItems = 0;
        modelData.userIdMap = new HashMap<>();
        modelData.itemIdMap = new HashMap<>();
        modelData.userIds = new ArrayList<>();
        modelData.itemIds = new ArrayList<>();

        for (Row row : rows) {
            long nodeId = getVertexId(row);
            boolean isUser = getIsUser(row);
            if (isUser) {
                modelData.userIdMap.put(nodeId, numUsers);
                modelData.userIds.add(nodeId);
                numUsers++;
            } else {
                modelData.itemIdMap.put(nodeId, numItems);
                modelData.itemIds.add(nodeId);
                numItems++;
            }
        }

        modelData.userFactors = new float[numUsers][0];
        modelData.itemFactors = new float[numItems][0];

        numUsers = 0;
        numItems = 0;

        for (Row row : rows) {
            boolean isUser = getIsUser(row);
            float[] factors = getFactors(row);
            if (isUser) {
                modelData.userFactors[numUsers] = factors;
                numUsers++;
            } else {
                modelData.itemFactors[numItems] = factors;
                numItems++;
            }
        }
        return modelData;
    }

    @Override
    public void save(Iterable<Tuple3<Byte, Long, float[]>> iterable, Collector<Row> collector) {
        for (Tuple3<Byte, Long, float[]> v : iterable) {
            collector.collect(createDataRow(v.f0, v.f1, v.f2));
        }
    }

    private static Row createDataRow(byte identity, long vertexId, float[] factors) {
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < factors.length; i++) {
            if (i > 0) {
                sbd.append(" ");
            }
            sbd.append(factors[i]);
        }
        if (identity == 0) {
            return Row.of(vertexId, null, sbd.toString());
        } else {
            return Row.of(null, vertexId, sbd.toString());
        }
    }
}
