package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.common.recommendation.SwingRecommModelConverter;
import com.alibaba.alink.operator.common.recommendation.SwingResData;
import com.alibaba.alink.params.recommendation.SwingTrainParams;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

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

        MultiStringIndexerTrainBatchOp stringIndexerModel = new MultiStringIndexerTrainBatchOp()
            .setMLEnvironmentId(mlEnvId)
            .setSelectedCols(userCol)
            .linkFrom(in);

        in = new HugeMultiStringIndexerPredictBatchOp()
            .setSelectedCols(userCol)
            .linkFrom(stringIndexerModel, in);

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

        DataSet<Row> itemCount = inputData
            .groupBy(new RowKeySelector(1))
            .reduceGroup(new CountItem(hasRateCol))
            .name("count_item");

        DataSet<Tuple3<Long, Tuple2<long[], float[]>, Float>> mainItemData = itemCount
            .groupBy(new RowKeySelector(0))
            .reduceGroup(new BuildSwingData())
            .name("build_main_item_data");

        DataSet<Row> itemSimilarity = mainItemData
            .groupBy(0)
            .reduceGroup(new CalcSimilarity(getAlpha(), getUserAlpha(), getUserBeta()))
            .name("reduce_operation");

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

        boolean hasRateCol;

        CountItem(boolean hasRateCol) {
            this.hasRateCol = hasRateCol;
        }

        @Override
        public void reduce(Iterable<Row> values, Collector<Row> out) throws Exception {
            float number = 0;
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
        implements GroupReduceFunction<Row, Tuple3<Long, Tuple2<long[], float[]>, Float>> {
        private static final long serialVersionUID = 6417591701594465880L;

        @Override
        public void reduce(Iterable<Row> values,
                           Collector<Tuple3<Long, Tuple2<long[], float[]>, Float>> out) throws Exception {
            ArrayList<Number[]> itemAndRateNum = new ArrayList<>();
            for (Row value : values) {
                itemAndRateNum.add(new Number[]{(long) value.getField(1), (float) value.getField(2)});
            }
            int localItemSize = itemAndRateNum.size();
            Tuple2<long[], float[]> otherItems = new Tuple2<>();
            otherItems.f0 = new long[localItemSize - 1];
            otherItems.f1 = new float[localItemSize - 1];
            for (int i = 0; i < localItemSize; i++) {
                long mainItem = (long) itemAndRateNum.get(i)[0];
                float mainItemRateNum = (float) itemAndRateNum.get(i)[1];
                int count = -1;
                for (int j = 0; j < localItemSize; j++) {
                    if (i == j) {
                        continue;
                    }
                    ++count;
                    otherItems.f0[count] = (long) itemAndRateNum.get(j)[0];
                    otherItems.f1[count] = (float) itemAndRateNum.get(j)[1];
                }
                out.collect(Tuple3.of(mainItem, otherItems, mainItemRateNum));
            }
        }
    }

    private static class CalcSimilarity
        extends RichGroupReduceFunction<Tuple3<Long, Tuple2<long[], float[]>, Float>, Row> {
        private static final long serialVersionUID = -2438120820385058339L;

        private final float alpha;
        private final float userAlpha;
        private final float userBeta;

        CalcSimilarity(float alpha, float userAlpha, float userBeta) {
            this.alpha = alpha;
            this.userAlpha = userAlpha;
            this.userBeta = userBeta;
        }

        @Override
        public void reduce(Iterable<Tuple3<Long, Tuple2<long[], float[]>, Float>> values,
                           Collector<Row> out) throws Exception {
            long mainItem = 0;
            List<Tuple2<HashSet<Long>, Float>> itemNeighborList = new ArrayList<>();
            for (Tuple3<Long, Tuple2<long[], float[]>, Float> value : values) {
                float thisUserRateNum = 0;
                mainItem = value.f0;
                int size = value.f1.f0.length;
                HashSet<Long> thisItems = new HashSet<>(size);
                Tuple2<long[], float[]> otherItems = value.f1;
                for (int i = 0; i < size; i++) {
                    long itemId = otherItems.f0[i];
                    thisItems.add(itemId);
                }
                thisUserRateNum += value.f2;
                itemNeighborList.add(Tuple2.of(thisItems, thisUserRateNum));
            }

            Tuple2<long[], float[]> itemsAndSimilarity = calcSimilarity(itemNeighborList, alpha);
            if (itemsAndSimilarity != null) {
                out.collect(Row.of(mainItem, itemsAndSimilarity.f0, itemsAndSimilarity.f1));
                if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
                    System.out.println("here in the " + getRuntimeContext().getIndexOfThisSubtask()
                        + "th task, main item " + mainItem + " ends reduce.");
                }
            }
        }

        private double calcUserWeight(double clk) {
            return Math.pow(userAlpha + clk, userBeta);
        }

        private Tuple2<long[], float[]> calcSimilarity(List<Tuple2<HashSet<Long>, Float>> itemNeighborList,
                                                       float alpha) {
            if (itemNeighborList.isEmpty()) {
                return null;
            }
            int size = itemNeighborList.size();
            HashMap<Long, Float> id2swing = new HashMap<>();
            for (int i = 0; i < size; i++) {
                double wi = calcUserWeight(itemNeighborList.get(i).f1);
                for (int j = i + 1; j < size; j++) {
                    double userWeight = wi * calcUserWeight(itemNeighborList.get(j).f1);
                    HashSet<Long> interaction = (HashSet<Long>) itemNeighborList.get(i).f0.clone();
                    interaction.retainAll(itemNeighborList.get(j).f0);
                    float interactionParam = interaction.size() + alpha;
                    for (Long id : interaction) {
                        float similarity = 0;
                        if (id2swing.containsKey(id)) {
                            similarity = id2swing.get(id);
                        }
                        similarity += userWeight / interactionParam;
                        id2swing.put(id, similarity);
                    }
                }
            }

            long[] resItems;
            float[] resSimilarity;
            int id2swingSize = id2swing.size();
            if (id2swingSize == 0) {
                return null;
            }
            int cnt = 0;
            resItems = new long[id2swingSize];
            resSimilarity = new float[id2swingSize];
            Number[][] resData = new Number[id2swingSize][2];
            for (Map.Entry<Long, Float> entry : id2swing.entrySet()) {
                Number[] entryData = new Number[2];
                entryData[0] = entry.getKey();
                entryData[1] = entry.getValue();
                resData[cnt] = entryData;
                ++cnt;
            }
            Arrays.sort(resData, new Comparator<Number[]>() {
                @Override
                public int compare(Number[] o1, Number[] o2) {
                    return ((Float) o2[1]).compareTo((Float) o1[1]);
                }
            });

            for (int i = 0; i < id2swingSize; i++) {
                resItems[i] = (long) resData[i][0];
                resSimilarity[i] = (float) resData[i][1];
            }
            return Tuple2.of(resItems, resSimilarity);
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

