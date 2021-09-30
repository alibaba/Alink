package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.SwingRecommModelConverter;
import com.alibaba.alink.operator.common.recommendation.SwingResData;
import com.alibaba.alink.params.recommendation.SwingTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Swing is an item recall model. The topology of user-item graph usually can be described as
 * user-item-user or item-user-item, which are like 'swing'.
 */
public class SwingTrainBatchOp extends BatchOperator<SwingTrainBatchOp>
    implements SwingTrainParams<SwingTrainBatchOp> {
    private static final long serialVersionUID = 6094224433980263495L;

    public SwingTrainBatchOp(Params params) {
        super(params);
    }

    public SwingTrainBatchOp() {
        this(new Params());
    }

    @Override
    public SwingTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        String userCol = getUserCol();
        String itemCol = getItemCol();
        String rateCol = getRateCol();
        Integer maxUserItems = getMaxUserItems();
        Integer minUserItems = getMinUserItems();
        Integer maxItemNumber = getMaxItemNumber();
        boolean normalize = getResultNormalize();
        boolean hasRateCol = rateCol != null;
        String[] selectedCols;
        if (hasRateCol) {
            selectedCols = new String[]{userCol, itemCol, rateCol};
        } else {
            selectedCols = new String[]{userCol, itemCol};
        }

        BatchOperator in = checkAndGetFirst(inputs)
            .select(selectedCols);
        long mlEnvId = getMLEnvironmentId();

        TypeInformation itemType = TableUtil.findColType(in.getSchema(), itemCol);

        DataSet<Row> distinctItem = in.select(itemCol)
            .getDataSet()
            .distinct(new RowKeySelector(0))
            .rebalance();

        DataSet<Tuple2<Long, Comparable>> indexAndItem = DataSetUtils.zipWithIndex(distinctItem)
            .mapPartition(new MapPartitionFunction <Tuple2 <Long, Row>, Tuple2 <Long, Comparable>>() {
                @Override
                public void mapPartition(Iterable <Tuple2 <Long, Row>> values,
                                         Collector <Tuple2 <Long, Comparable>> out) throws Exception {
                    values.forEach(x -> out.collect(Tuple2.of(x.f0, (Comparable) x.f1.getField(0))));
                }
            });

        int itemIndex = TableUtil.findColIndexWithAssertAndHint(in.getSchema(), itemCol);

        DataSet<Row> inputData = in.getDataSet()
            .mapPartition(new Item2Index(itemIndex))
            .withBroadcastSet(indexAndItem, "indexAndItem")
            .name("map_item_data");

        //存储item ID，同用户其他item ID
        DataSet<Tuple2<Long, Long[]>> mainItemData = inputData
            .groupBy(new RowKeySelector(0))
            .reduceGroup(new BuildSwingData(maxUserItems, minUserItems))
            .name("build_main_item_data");

        DataSet<Row> itemSimilarity = mainItemData
            .groupBy(0)
            .reduceGroup(new CalcSimilarity(getAlpha(), maxItemNumber, getUserAlpha(), getUserBeta(), normalize))
            .name("compute_similarity");

        DataSet<Row> modelData = itemSimilarity
            .mapPartition(new BuildModelData(itemCol))
            .withBroadcastSet(indexAndItem, "indexAndItem")
            .name("build_model_data");

        this.setOutput(modelData, new SwingRecommModelConverter(itemType).getModelSchema());
        return this;
    }

    private static class Item2Index extends RichMapPartitionFunction<Row, Row> {
        int itemIndex;
        private Map<Comparable, Long> itemIndexMap;

        Item2Index(int itemIndex) {
            this.itemIndex = itemIndex;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<Tuple2<Long, Comparable>> indexAndItem = getRuntimeContext()
                .getBroadcastVariable("indexAndItem");
            itemIndexMap = new HashMap<>(indexAndItem.size());
            for (Tuple2<Long, Comparable> tuple2 : indexAndItem) {
                itemIndexMap.put(tuple2.f1, tuple2.f0);
            }
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
            for (Row value : values) {
                value.setField(itemIndex, itemIndexMap.get(value.getField(itemIndex)));
                out.collect(value);
            }
        }
    }

    public static class RowKeySelector implements KeySelector<Row, Comparable> {
        private static final long serialVersionUID = 7514280642434354647L;
        int index;

        public RowKeySelector(int index) {
            this.index = index;
        }

        @Override
        public Comparable getKey(Row value) {
            return (Comparable) value.getField(index);
        }
    }

    /**
     * Count the total number of each item.
     */
    private static class CountItem implements GroupReduceFunction<Row, Row> {
        private static final long serialVersionUID = -7426335460720352495L;

        CountItem() {}

        @Override
        public void reduce(Iterable<Row> values, Collector<Row> out) throws Exception {
            int number = 0;
            List<Row> valueSave = new ArrayList<>();
            for (Row value : values) {
                ++number;
                valueSave.add(value);
            }

            Row res = new Row(3);
            for (Row value : valueSave) {
                res.setField(0, value.getField(0));
                res.setField(1, value.getField(1));
                res.setField(2, number);
                out.collect(res);
            }
        }
    }

    /**
     * group by user col.
     */
    private static class BuildSwingData
        implements GroupReduceFunction<Row, Tuple2<Long, Long[]>> {
        private static final long serialVersionUID = 6417591701594465880L;

        int maxUserItems;
        int minUserItems;

        BuildSwingData(int maxUserItems, int minUserItems) {
            this.maxUserItems = maxUserItems;
            this.minUserItems = minUserItems;
        }

        @Override
        public void reduce(Iterable<Row> values,
                           Collector<Tuple2<Long, Long[]>> out) throws Exception {
            ArrayList<Long> userItemList = new ArrayList<>();
            for (Row value : values) {
                userItemList.add((Long) value.getField(1));
            }
            if (userItemList.size() < this.minUserItems || userItemList.size() > this.maxUserItems) {
                return;
            }
            Long[] userItemIDs = new Long[userItemList.size()];
            for (int i = 0; i < userItemList.size(); i++) {
                userItemIDs[i] = userItemList.get(i);
            }
            for (int i = 0; i < userItemIDs.length; i++) {
                out.collect(Tuple2.of(userItemIDs[i], userItemIDs));
            }
        }
    }

    private static class CalcSimilarity
        extends RichGroupReduceFunction<Tuple2<Long, Long[]>, Row> {
        private static final long serialVersionUID = -2438120820385058339L;

        private final float alpha;
        int maxItemNumber;
        float userAlpha;
        float userBeta;
        boolean normalize;

        CalcSimilarity(float alpha, int maxItemNumber, float userAlpha, float userBeta, boolean normalize) {
            this.alpha = alpha;
            this.userAlpha = userAlpha;
            this.userBeta = userBeta;
            this.maxItemNumber = maxItemNumber;
            this.normalize = normalize;
        }

        private float computeUserWeight(int size) {
            return (float)(1.0 / Math.pow(userAlpha + size, userBeta));
        }

        @Override
        public void reduce(Iterable<Tuple2<Long, Long[]>> values,
                           Collector<Row> out) throws Exception {
            Long mainItem = null;
            ArrayList<Long[]> dataList = new ArrayList <>();
            for (Tuple2<Long, Long[]> value : values) {
                mainItem = value.f0;
                if (dataList.size() == this.maxItemNumber) {
                    int randomIndex = (int)(Math.random() * (this.maxItemNumber + 1));
                    if (randomIndex < this.maxItemNumber) {
                        dataList.set(randomIndex, value.f1);
                    }
                } else {
                    dataList.add(value.f1);
                }
            }
            ArrayList <HashSet <Long>> itemSetList = new ArrayList <>(dataList.size());
            Float[] userWeights = new Float[dataList.size()];
            int weightIndex = 0;
            for (Long[] value : dataList) {
                HashSet <Long> itemSet = new HashSet <>(value.length);
                itemSet.addAll(Arrays.asList(value));
                itemSetList.add(itemSet);
                userWeights[weightIndex++] = computeUserWeight(value.length);
            }

            //双重遍历，计算swing权重
            HashMap <Long, Float> id2swing = new HashMap <>();
            for (int i = 0; i < itemSetList.size(); i++) {
                for (int j = i + 1; j < itemSetList.size(); j++) {
                    HashSet <Long> interaction = (HashSet <Long>) itemSetList.get(i).clone();
                    interaction.retainAll(itemSetList.get(j));
                    if (interaction.size() == 0) {
                        continue;
                    }
                    float similarity = userWeights[i] * userWeights[j] / (alpha + interaction.size());
                    for (Long id : interaction) {
                        if (id.equals(mainItem)) {
                            continue;
                        }
                        float itemSimilarity = id2swing.getOrDefault(id, (float) 0) + similarity;
                        id2swing.put(id, itemSimilarity);
                    }
                }
            }
            ArrayList<Tuple2<Long, Float>> itemAndScore = new ArrayList<>();
            id2swing.forEach(
                (key, value) -> itemAndScore.add(Tuple2.of(key, value))
            );

            itemAndScore.sort(new Comparator <Tuple2 <Long, Float>>() {
                @Override
                public int compare(Tuple2 <Long, Float> o1, Tuple2 <Long, Float> o2) {
                    return 0 - Float.compare(o1.f1, o2.f1);
                }
            });
            long[] itemIds = new long[itemAndScore.size()];
            float[] itemScores = new float[itemAndScore.size()];
            float maxScore = this.normalize ? itemAndScore.get(0).f1 : 1.0f;
            for (int i = 0; i < itemAndScore.size(); i++) {
                itemIds[i] = itemAndScore.get(i).f0;
                itemScores[i] = itemAndScore.get(i).f1 / maxScore;
            }
            out.collect(Row.of(mainItem, itemIds, itemScores));
        }
    }

    private static class BuildModelData extends RichMapPartitionFunction<Row, Row> {
        private Comparable[] originalItems;
        private final String itemCol;
        BuildModelData(String itemCol) {
            this.itemCol = itemCol;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            List<Tuple2<Long, Comparable>> indexAndItem =
                getRuntimeContext().getBroadcastVariable("indexAndItem");
            int itemNumber = indexAndItem.size();
            originalItems = new Comparable[itemNumber];
            for (Tuple2<Long, Comparable> tuple2 : indexAndItem) {
                originalItems[tuple2.f0.intValue()] = tuple2.f1;
            }
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
            for (Row value : values) {
                long mainItemIndex = (long) value.getField(0);
                Comparable originMainItem = originalItems[(int) mainItemIndex];
                long[] items = (long[]) value.getField(1);
                float[] similarity = (float[]) value.getField(2);
                Comparable[] originItems = new Comparable[items.length];
                for (int i = 0; i < items.length; i++) {
                    originItems[i] = originalItems[(int) items[i]];
                }
                SwingResData resData = new SwingResData(originItems, similarity, itemCol);
                out.collect(Row.of(
                    originMainItem,
                    JsonConverter.toJson(resData)));
            }
        }
    }
}

