package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * Example for PrefixSpan.
 */
public class PrefixSpanExample {
    @Test
    public void main() throws Exception {
        Row[] rows = new Row[]{
            Row.of("a;a,b,c;a,c;d;c,f"),
            Row.of("a,d;c;b,c;a,e"),
            Row.of("e,f;a,b;d,f;c;b"),
            Row.of("e;g;a,f;c;b;c"),
        };

        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[]{"sequence"})
        );

        PrefixSpanBatchOp prefixSpan = new PrefixSpanBatchOp()
            .setItemsCol("sequence")
            .setMinSupportCount(2);

        prefixSpan.linkFrom(data).print();
    }
}
