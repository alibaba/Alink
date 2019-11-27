package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link EstimatorBase}.
 */
public class EstimatorBaseTest extends PipelineStageTestBase {

    /**
     * This fake estimator simply record which fit method is invoked.
     */
    private static class FakeEstimator extends EstimatorBase {

        boolean batchFitted = false;
        boolean streamFitted = false;

        @Override
        public ModelBase fit(BatchOperator input) {
            batchFitted = true;
            return null;
        }

        @Override
        public ModelBase fit(StreamOperator input) {
            streamFitted = true;
            return null;
        }
    }

    @Override
    protected PipelineStageBase createPipelineStage() {
        return new FakeEstimator();
    }

    @Test
    public void testFitBatchTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataSet<Integer> input= env.getExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getBatchTableEnvironment().fromDataSet(input);

        FakeEstimator estimator = new FakeEstimator();
        estimator.setMLEnvironmentId(id);
        estimator.fit(env.getBatchTableEnvironment(), table);

        Assert.assertTrue(estimator.batchFitted);
        Assert.assertFalse(estimator.streamFitted);
    }


    @Test
    public void testFitStreamTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataStream<Integer> input= env.getStreamExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getStreamTableEnvironment().fromDataStream(input);

        FakeEstimator estimator = new FakeEstimator();
        estimator.setMLEnvironmentId(id);
        estimator.fit(env.getStreamTableEnvironment(), table);

        Assert.assertFalse(estimator.batchFitted);
        Assert.assertTrue(estimator.streamFitted);
    }
}