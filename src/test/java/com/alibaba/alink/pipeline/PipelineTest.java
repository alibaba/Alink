package com.alibaba.alink.pipeline;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PipelineTest {

    /**
     * Create a mocked transformer which return the input identically.
     *
     * @param name name of the transformer
     * @return the mocked transformer
     */
    private static TransformerBase mockTransformer(String name) {
        TransformerBase transformer = mock(TransformerBase.class, name);
        when(transformer.transform(any(BatchOperator.class)))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        when(transformer.transform(any(StreamOperator.class)))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        return transformer;
    }

    /**
     * Create a mocked estimator and model pair. The mocked model will return the input identically.
     *
     * @param name name postfix of estimator.
     * @return mocked estimator and model pair.
     */
    private static Pair<EstimatorBase, ModelBase> mockEstimator(String name) {
        ModelBase model = mock(ModelBase.class, "model_" + name);
        EstimatorBase estimator = mock(EstimatorBase.class, "estimator_" + name);
        when(estimator.fit(any(BatchOperator.class))).thenReturn(model);
        when(estimator.fit(any(StreamOperator.class))).thenReturn(model);
        when(model.transform(any(BatchOperator.class)))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        when(model.transform(any(StreamOperator.class)))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        return ImmutablePair.of(estimator, model);
    }

    @Test
    public void testFit() {
        BatchOperator data = new MemSourceBatchOp(new Object[]{1}, "colName");

        TransformerBase stage1 = mockTransformer("stage1");
        TransformerBase stage2 = mockTransformer("stage2");
        Pair<EstimatorBase, ModelBase> stage3 = mockEstimator("stage3");
        TransformerBase stage4 = mockTransformer("stage4");
        Pair<EstimatorBase, ModelBase> stage5 = mockEstimator("stage5");
        TransformerBase stage6 = mockTransformer("stage6");

        Pipeline pipe = new Pipeline().add(stage1).add(stage2).add(stage3.getLeft())
            .add(stage4).add(stage5.getLeft()).add(stage6);
        pipe.fit(data);

        // The transform methods of the first 2 transformers should be invoked.
        // because they are expected transform input data to fit estimators.
        verify(stage1, times(1)).transform(any(BatchOperator.class));
        verify(stage2, times(1)).transform(any(BatchOperator.class));

        // Verify that estimator of stage 3 is fitted.
        verify(stage3.getLeft(), times(1)).fit(any(BatchOperator.class));
        // And the generated model is used to transform data for estimator on stage 5.
        verify(stage3.getRight(), times(1)).transform(any(BatchOperator.class));

        verify(stage4, times(1)).transform(any(BatchOperator.class));

        // Verify that estimator of stage 5 is fitted.
        verify(stage5.getLeft(), times(1)).fit(any(BatchOperator.class));
        // But we don't have to transform data with the generated model.
        verify(stage5.getRight(), never()).transform(any(BatchOperator.class));

        verify(stage6, never()).transform(any(BatchOperator.class));
    }


    @Test
    public void testFitWithoutEstimators() {
        BatchOperator data = new MemSourceBatchOp(new Object[]{1}, "colName");

        TransformerBase stage1 = mockTransformer("stage1");
        TransformerBase stage2 = mockTransformer("stage2");
        TransformerBase stage3 = mockTransformer("stage3");
        TransformerBase stage4 = mockTransformer("stage4");

        Pipeline pipe = new Pipeline().add(stage1).add(stage2).add(stage3).add(stage4);
        pipe.fit(data);

        // Never need to transform data since there're no estimators.
        verify(stage1, never()).transform(any(BatchOperator.class));
        verify(stage2, never()).transform(any(BatchOperator.class));
        verify(stage3, never()).transform(any(BatchOperator.class));
        verify(stage4, never()).transform(any(BatchOperator.class));
    }

}