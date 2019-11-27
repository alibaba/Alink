package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * Example for FpGrowth.
 */
public class FpGrowthExample {

    @Test
    public void main() throws Exception {
        Row[] rows = new Row[]{
            Row.of("A,B,C,D"),
            Row.of("B,C,E"),
            Row.of("A,B,C,E"),
            Row.of("B,D,E"),
            Row.of("A,B,C,D"),
        };

        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[]{"items"})
        );

        FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
            .setItemsCol("items")
            .setMinSupportPercent(0.4)
            .setMinConfidence(0.6);

        fpGrowth.linkFrom(data).print();
    }
}
