package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Map;

/**
 * Unit test for ClassificationEvaluationUtil.
 */
public class ClassificationEvaluationUtilTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void judgeEvaluationTypeTest(){
        Params params = new Params()
            .set(HasPredictionDetailCol.PREDICTION_DETAIL_COL, "detail");

        ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(params);
        Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_DETAIL);

        params.set(HasPredictionCol.PREDICTION_COL, "pred");
        type = ClassificationEvaluationUtil.judgeEvaluationType(params);
        Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_DETAIL);

        params.remove(HasPredictionDetailCol.PREDICTION_DETAIL_COL);
        type = ClassificationEvaluationUtil.judgeEvaluationType(params);
        Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_RESULT);

        params.remove(HasPredictionCol.PREDICTION_COL);
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Error Input, must give either predictionCol or predictionDetailCol!");
        ClassificationEvaluationUtil.judgeEvaluationType(params);
    }

    @Test
    public void predResultLabelMapException() {
        HashSet<String> set = new HashSet <>();
        set.add("0");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The distinct label number less than 2!");
        ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, true, null);
    }

    @Test
    public void predResultLabelMapTest() {
        HashSet <String> set = new HashSet <>();
        set.add("1");
        set.add("0");
        Tuple2<Map<String, Integer>, String[]> tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, true, null);
        Map<String, Integer> map = tuple2.f0;
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals((int) map.get("0"), 1);
        Assert.assertEquals((int) map.get("1"), 0);

        tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, true, "0");
        map = tuple2.f0;
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals((int) map.get("0"), 0);
        Assert.assertEquals((int) map.get("1"), 1);

        set.add("2");
        tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, false, null);
        map = tuple2.f0;
        Assert.assertEquals(map.size(), 3);
        Assert.assertEquals((int) map.get("0"), 2);
        Assert.assertEquals((int) map.get("1"), 1);
        Assert.assertEquals((int) map.get("2"), 0);

        Assert.assertArrayEquals(new String[]{"2", "1", "0"}, tuple2.f1);
    }
}