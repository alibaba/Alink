package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.clustering.LdaModelData;
import com.alibaba.alink.operator.common.clustering.LdaModelDataConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class LdaModelDataConverterTest {

    private DenseMatrix denseMatrix = new DenseMatrix(new double[][]
            {{576.6534038086453, 484.876383945505, 562.4320336284154, 667.3878438751825, 633.2160319763818},
                    {595.9480191131199, 614.3523764902384, 669.9956643558695, 625.9405747355178, 545.404484026171},
                    {628.0710235806204, 566.4834369032806, 620.0388681745285, 522.2145900650611, 473.45782971290805}});

    private LdaModelDataConverter converter = new LdaModelDataConverter();

    private List<String> generateDocData() {
        List<String> docData = new ArrayList<>();
        docData.add("{\"f0\":\"b\",\"f1\":0.0,\"f2\":0}");
        docData.add("{\"f0\":\"c\",\"f1\":0.5108256237659907,\"f2\":1}");
        docData.add("{\"f0\":\"a\",\"f1\":0.0,\"f2\":2}");
        docData.add("{\"f0\":\"d\",\"f1\":0.5108256237659907,\"f2\":3}");
        docData.add("{\"f0\":\"e\",\"f1\":0.9162907318741551,\"f2\":4}");
        return docData;
    }

    private LdaModelData generateLdaModelData() {
        LdaModelData modelData = new LdaModelData();
        modelData.gamma = denseMatrix;
        modelData.alpha = new Double[]{0.2, 0.2, 0.2, 0.2, 0.2};
        modelData.beta = new Double[]{0.2, 0.2, 0.2, 0.2, 0.2};
        modelData.topicNum = 3;
        modelData.vocabularySize = 5;
        modelData.optimizer = "em";
        modelData.list = generateDocData();
        modelData.gamma = denseMatrix;
        return modelData;
    }

    @Test
    public void ldaModelDataConverterTest() {

        Tuple2<Params, Iterable<String>> res = converter.serializeModel(generateLdaModelData());
        LdaModelData modelData = converter.deserializeModel(res.f0, res.f1);
        assertEquals(modelData.alpha, new Double[]{0.2, 0.2, 0.2, 0.2, 0.2});
        assertEquals(modelData.list, generateDocData());

    }
}