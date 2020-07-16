package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for QuantileDiscretizerModelDataConverter.
 */
public class QuantileDiscretizerModelDataConverterTest {

    @Test
    public void testAssembledVector() throws Exception {
        QuantileDiscretizerModelDataConverter quantileModel = new QuantileDiscretizerModelDataConverter();

        quantileModel.load(QuantileDiscretizerModelMapperTest.model);

        System.out.println(quantileModel.getFeatureValue("col2", 0));
        System.out.println(quantileModel.getFeatureSize("col2"));
        System.out.println(quantileModel.missingIndex("col2"));
    }
}