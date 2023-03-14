package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeIndexerStringPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.HugeStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.recommendation.SwingRecommKernel;
import com.alibaba.alink.operator.common.recommendation.SwingRecommModelConverter;
import com.alibaba.alink.operator.common.recommendation.SwingResData;
import com.alibaba.alink.params.recommendation.SwingTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Swing is an item recall model. The topology of user-item graph usually can be described as
 * user-item-user or item-user-item, which are like 'swing'.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
    @PortSpec(PortType.MODEL)
})
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
@NameCn("swing训练")
@NameEn("Swing Recommendation Training")
public class SwingTrainBatchOp extends BatchOperator <SwingTrainBatchOp>
    implements SwingTrainParams <SwingTrainBatchOp> {
    private static final long serialVersionUID = 6094224433980263495L;
    private static final String ITEM_ID_COLNAME = "alink_itemID_in_swing";

    public SwingTrainBatchOp(Params params) {
        super(params);
    }

    public SwingTrainBatchOp() {
        this(new Params());
    }

    @Override
    public SwingTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
        String userCol = getUserCol();
        String itemCol = getItemCol();
        Integer maxUserItems = getMaxUserItems();
        Integer minUserItems = getMinUserItems();
        Integer maxItemNumber = getMaxItemNumber();
        boolean normalize = getResultNormalize();
        String[] selectedCols = new String[] {userCol, itemCol};

        BatchOperator <?> in = checkAndGetFirst(inputs)
            .select(selectedCols);
        long mlEnvId = getMLEnvironmentId();

        TypeInformation <?> itemType = TableUtil.findColType(in.getSchema(), itemCol);
        if (!itemType.equals(Types.STRING) && !itemType.equals(Types.INT) && !itemType.equals(Types.LONG)) {
            throw new AkIllegalDataException("not supported item type:" + itemType + ", should be int,long or string");
        }
        int idIndex = 1;
        StringIndexerTrainBatchOp model = new StringIndexerTrainBatchOp();
        if (itemType.equals(Types.STRING)) {
            model.setSelectedCol(itemCol)
                .setMLEnvironmentId(mlEnvId)
                .setStringOrderType("random")
                .linkFrom(in);

            in = new HugeStringIndexerPredictBatchOp()
                .setSelectedCols(itemCol)
                .setOutputCols(ITEM_ID_COLNAME)
                .setMLEnvironmentId(mlEnvId)
                .linkFrom(model, in);
            idIndex = 2;
        }

        //存储item ID，同用户其他item ID
        DataSet <Tuple3 <Comparable <?>, Long, Long[]>> mainItemData = in.getDataSet()
            .groupBy(new RowKeySelector(0))
            .reduceGroup(new BuildSwingData(maxUserItems, minUserItems, idIndex))
            .name("build_main_item_data");

        DataSet <Row> itemSimilarity = mainItemData
            .groupBy(1)
            .reduceGroup(
                new CalcSimilarity(getAlpha(), maxItemNumber, maxUserItems, getUserAlpha(), getUserBeta(), normalize))
            .name("compute_similarity");

        BatchOperator <?> itemResult = BatchOperator.fromTable(
            DataSetConversionUtil.toTable(getMLEnvironmentId(), itemSimilarity,
                new String[] {itemCol, "swing_items", "swing_scores"},
                new TypeInformation[] {itemType, Types.OBJECT_ARRAY(Types.LONG), Types.OBJECT_ARRAY(Types.FLOAT)}
            ));

        if (itemType.equals(Types.STRING)) {
            itemResult = new HugeIndexerStringPredictBatchOp()
                .setSelectedCols("swing_items")
                .setMLEnvironmentId(mlEnvId)
                .linkFrom(model, itemResult);
        }
        Params meta = getParams().set(SwingRecommKernel.ITEM_TYPE, FlinkTypeConverter.getTypeString(itemType));
        DataSet <Row> modelData = itemResult.getDataSet()
            .mapPartition(new BuildModelData(itemCol, meta))
            .name("build_model_data");

        this.setOutput(modelData, new SwingRecommModelConverter(itemType).getModelSchema());
        return this;
    }

    public static class RowKeySelector implements KeySelector <Row, Comparable <?>> {
        private static final long serialVersionUID = 7514280642434354647L;
        int index;

        public RowKeySelector(int index) {
            this.index = index;
        }

        @Override
        public Comparable <?> getKey(Row value) {
            return (Comparable <?>) value.getField(index);
        }
    }

    /**
     * group by user col.
     */
    private static class BuildSwingData
        implements GroupReduceFunction <Row, Tuple3 <Comparable <?>, Long, Long[]>> {
        private static final long serialVersionUID = 6417591701594465880L;

        int maxUserItems;
        int minUserItems;
        int idIndex;

        BuildSwingData(int maxUserItems, int minUserItems, int idIndex) {
            this.maxUserItems = maxUserItems;
            this.minUserItems = minUserItems;
            this.idIndex = idIndex;
        }

        @Override
        public void reduce(Iterable <Row> values,
                           Collector <Tuple3 <Comparable <?>, Long, Long[]>> out) throws Exception {
            HashMap <Long, Comparable <?>> userItemMap = new HashMap <>();
            for (Row value : values) {
                userItemMap.put(Long.valueOf(String.valueOf(value.getField(idIndex))),
                    (Comparable <?>) value.getField(1));
            }
            if (userItemMap.size() < this.minUserItems || userItemMap.size() > this.maxUserItems) {
                return;
            }
            Long[] userItemIDs = new Long[userItemMap.size()];
            int index = 0;
            for (Entry <Long, Comparable <?>> pair : userItemMap.entrySet()) {
                userItemIDs[index++] = pair.getKey();
            }
            for (Long userItemID : userItemIDs) {
                out.collect(Tuple3.of(userItemMap.get(userItemID), userItemID, userItemIDs));
            }
        }
    }

    private static class CalcSimilarity
        extends RichGroupReduceFunction <Tuple3 <Comparable <?>, Long, Long[]>, Row> {
        private static final long serialVersionUID = -2438120820385058339L;

        private final float alpha;
        int maxItemNumber;
        int maxUserItems;
        float userAlpha;
        float userBeta;
        boolean normalize;

        CalcSimilarity(float alpha, int maxItemNumber, int maxUserItems, float userAlpha, float userBeta,
                       boolean normalize) {
            this.alpha = alpha;
            this.userAlpha = userAlpha;
            this.userBeta = userBeta;
            this.maxItemNumber = maxItemNumber;
            this.maxUserItems = maxUserItems;
            this.normalize = normalize;
        }

        private float computeUserWeight(int size) {
            return (float) (1.0 / Math.pow(userAlpha + size, userBeta));
        }

        @Override
        public void reduce(Iterable <Tuple3 <Comparable <?>, Long, Long[]>> values,
                           Collector <Row> out) throws Exception {
            Comparable <?> item = null;
            Long mainItem = null;
            ArrayList <Long[]> dataList = new ArrayList <>();
            for (Tuple3 <Comparable <?>, Long, Long[]> value : values) {
                item = value.f0;
                mainItem = value.f1;
                if (dataList.size() == this.maxItemNumber) {
                    int randomIndex = (int) (Math.random() * (this.maxItemNumber + 1));
                    if (randomIndex < this.maxItemNumber) {
                        dataList.set(randomIndex, value.f2);
                    }
                } else {
                    dataList.add(value.f2);
                }
            }
            float[] userWeights = new float[dataList.size()];
            int weightIndex = 0;
            for (Long[] value : dataList) {
                Arrays.sort(value);
                userWeights[weightIndex++] = computeUserWeight(value.length);
            }

            //双重遍历，计算swing权重
            HashMap <Long, Float> id2swing = new HashMap <>();
            long[] interaction = new long[maxUserItems];
            for (int i = 0; i < dataList.size(); i++) {
                for (int j = i + 1; j < dataList.size(); j++) {
                    int interactionSize = countCommonItems(dataList.get(i), dataList.get(j), interaction);
                    if (interactionSize == 0) {
                        continue;
                    }
                    float similarity = userWeights[i] * userWeights[j] / (alpha + interactionSize);
                    for (int k = 0; k < interactionSize; k++) {
                        Long id = interaction[k];
                        if (id.equals(mainItem)) {
                            continue;
                        }
                        float itemSimilarity = id2swing.getOrDefault(id, (float) 0) + similarity;
                        id2swing.put(id, itemSimilarity);
                    }
                }
            }
            ArrayList <Tuple2 <Long, Float>> itemAndScore = new ArrayList <>();
            id2swing.forEach(
                (key, value) -> itemAndScore.add(Tuple2.of(key, value))
            );

            itemAndScore.sort(new Comparator <Tuple2 <Long, Float>>() {
                @Override
                public int compare(Tuple2 <Long, Float> o1, Tuple2 <Long, Float> o2) {
                    return 0 - Float.compare(o1.f1, o2.f1);
                }
            });
            if (itemAndScore.size() == 0) {
                return;
            }
            Long[] itemIds = new Long[itemAndScore.size()];
            Float[] itemScores = new Float[itemAndScore.size()];
            float maxScore = this.normalize ? itemAndScore.get(0).f1 : 1.0f;
            for (int i = 0; i < itemAndScore.size(); i++) {
                itemIds[i] = itemAndScore.get(i).f0;
                itemScores[i] = itemAndScore.get(i).f1 / maxScore;
            }
            out.collect(Row.of(item, itemIds, itemScores));
        }

        private static int countCommonItems(Long[] u, Long[] v, long[] interaction) {
            int pointerU = 0;
            int pointerV = 0;
            int interactionSize = 0;
            while (pointerU < u.length && pointerV < v.length) {
                if (u[pointerU].equals(v[pointerV])) {
                    interaction[interactionSize++] = u[pointerU];
                    pointerU++;
                    pointerV++;
                } else if (u[pointerU] < v[pointerV]) {
                    pointerU++;
                } else {
                    pointerV++;
                }
            }
            return interactionSize;
        }
    }

    private static class BuildModelData extends RichMapPartitionFunction <Row, Row> {
        private final String itemCol;
        private final Params meta;

        BuildModelData(String itemCol, Params meta) {
            this.itemCol = itemCol;
            this.meta = meta;
        }

        @Override
        public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                out.collect(Row.of(null, meta.toJson()));
            }
            for (Row value : values) {
                Comparable <?> originMainItem = (Comparable <?>) value.getField(0);
                Object itemsValue = value.getField(1);
                Object[] items;
                if (itemsValue instanceof String) {
                    items = ((String) value.getField(1)).split(",");
                } else {
                    items = (Long[]) value.getField(1);
                }
                Float[] similarity = (Float[]) value.getField(2);
                SwingResData resData = new SwingResData(items, similarity, itemCol);
                out.collect(Row.of(
                    originMainItem,
                    JsonConverter.toJson(resData)));
            }
        }
    }
}