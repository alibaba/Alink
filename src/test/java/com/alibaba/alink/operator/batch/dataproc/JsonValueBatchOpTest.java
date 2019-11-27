package com.alibaba.alink.operator.batch.dataproc;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class JsonValueBatchOpTest {
    @Test
    public void test() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of(new Object[] {"{\"ak\":'asdfa',\"ck\":2}"}),
            };
        String[] colnames = new String[] {"jsoncol"};
        MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
        List<Row> result = inOp.link(new JsonValueBatchOp()
            .setSkipFailed(true)
            .setSelectedCol("jsoncol").setOutputCols(new String[] {"jsonval", "jv"})
            .setJsonPath(new String[] {"$.ak", "$.ck"})).collect();

        Assert.assertEquals(result.get(0).getField(1).toString(), "asdfa");
        Assert.assertEquals(result.get(0).getField(2).toString(), "2");
    }

    @Test
    public void test1() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of(new Object[] {
                    "{\"creatorCid\":36821, \"creatorId\":74769, id:60309766, scope:public, sourceApp:O1000001, "
                        + "sourceScene:WISH_COMMENT, sourceSceneId:59977198, tag:O1000001_WISH_COMMENT}",
                    "{actor:74769, eventName:feedPublished, time:1.561599896515E12}"}),
                Row.of(new Object[] {
                    "{\"creatorCid\":36821, \"creatorId\":169604, id:60286818, scope:private, sourceApp:O1000001, "
                        + "sourceScene:main_wall, sourceSceneId:, tag:O1000001_main_wall}",
                    "{actor:144636, eventName:feedSinged, time:1.561556213999E12}"}),
            };
        String[] colnames = new String[] {"c1", "c2"};
        MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);
        inOp.link(new JsonValueStreamOp()
            .setSkipFailed(false)
            .setSelectedCol("c1").setOutputCols(new String[] {"jsonval"})
            .setJsonPath(new String[] {"$.creatorCid"})).print();
        StreamOperator.execute();
    }

}