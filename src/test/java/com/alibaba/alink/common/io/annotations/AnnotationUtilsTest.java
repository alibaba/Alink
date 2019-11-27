package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.operator.AlgoOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AnnotationUtilsTest {
    @DBAnnotation(name = "test_fake_db_1")
    private static class FakeDB1 extends FakeDBBase {
        public FakeDB1(Params params) {
            super(params);
        }
    }

    @DBAnnotation(name = "test_fake_db_2", hasTimestamp = true, tableNameAlias = "topic")
    private static class FakeDB2 extends FakeDBBase {
        public FakeDB2(Params params) {
            super(params);
        }
    }

    private static class FakeDB3 extends FakeDBBase {
        protected FakeDB3(Params params) {
            super(params);
        }
    }

    @IoOpAnnotation(name = "test_fake_op_1", ioType = IOType.SourceBatch)
    private static class FakeOp1 extends FakeOpBase {
        public FakeOp1(Params params) {
            super(params);
        }
    }

    @IoOpAnnotation(name = "test_fake_op_2", ioType = IOType.SourceBatch, hasTimestamp = true)
    private static class FakeOp2 extends FakeOpBase {
        public FakeOp2(Params params) {
            super(params);
        }
    }

    public static class FakeOp3 extends FakeOpBase {
        public FakeOp3(Params params) {
            super(params);
        }
    }

    @DBAnnotation(name = "dummy1")
    private static class DummyClass1 {
    }

    @IoOpAnnotation(name = "dummy2", ioType = IOType.SourceBatch)
    private static class DummyClass2 {
    }

    @Test
    public void testAnnotatedName() {
        Assert.assertEquals("test_fake_db_1", AnnotationUtils.annotatedName(FakeDB1.class));
        Assert.assertEquals("test_fake_db_2", AnnotationUtils.annotatedName(FakeDB2.class));
        Assert.assertEquals("test_fake_op_1", AnnotationUtils.annotatedName(FakeOp1.class));
        Assert.assertEquals("test_fake_op_2", AnnotationUtils.annotatedName(FakeOp2.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testAnnotatedNameException() {
        AnnotationUtils.annotatedName(DummyClass1.class);
    }

    @Test
    public void testAnnotatedAlias() {
        Assert.assertEquals("tableName", AnnotationUtils.annotatedAlias(FakeDB1.class));
        Assert.assertEquals("topic", AnnotationUtils.annotatedAlias(FakeDB2.class));
        Assert.assertNull(AnnotationUtils.annotatedAlias(FakeDB3.class));
    }

    @Test
    public void testAnnotatedAliasByName() {
        Assert.assertEquals("tableName", AnnotationUtils.annotatedAlias("test_fake_db_1"));
        Assert.assertEquals("topic", AnnotationUtils.annotatedAlias("test_fake_db_2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnnotatedAliasByNameError() {
        Assert.assertNull(AnnotationUtils.annotatedAlias("A_DB_HAS_NO_NAME"));
    }

    @Test
    public void testAnnotatedIoType() {
        Assert.assertEquals(IOType.SourceBatch, AnnotationUtils.annotatedIoType(FakeOp1.class));
        Assert.assertEquals(IOType.SourceBatch, AnnotationUtils.annotatedIoType(FakeOp2.class));
        Assert.assertNull(AnnotationUtils.annotatedIoType(FakeOp3.class));
    }

    @Test
    public void testGetTableAliasParamKeyByName() {
        ParamInfo<String> param = AnnotationUtils.tableAliasParamKey("test_fake_db_1");
        Assert.assertEquals("DbKey: tableName", param.getDescription());
        Assert.assertEquals("tableName", param.getName());

        param = AnnotationUtils.tableAliasParamKey("test_fake_db_2");
        Assert.assertEquals("DbKey: topic", param.getDescription());
        Assert.assertEquals("topic", param.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTableAliasParamKeyByNameError() {
        AnnotationUtils.tableAliasParamKey("A_DB_HAS_NO_NAME");
    }

    @Test
    public void testGetTableAliasParamKeyByClass() {
        ParamInfo<String> param = AnnotationUtils.tableAliasParamKey(FakeDB1.class);
        Assert.assertEquals("DbKey: tableName", param.getDescription());
        Assert.assertEquals("tableName", param.getName());

        param = AnnotationUtils.tableAliasParamKey(FakeDB2.class);
        Assert.assertEquals("DbKey: topic", param.getDescription());
        Assert.assertEquals("topic", param.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTableAliasParamKeyByClassError() {
        AnnotationUtils.tableAliasParamKey(FakeDB3.class);
    }

    @Test
    public void testDynamicParamKey() {
        ParamInfo<String> param = AnnotationUtils.dynamicParamKey("key");
        Assert.assertEquals("DbKey: key", param.getDescription());
        Assert.assertEquals("key", param.getName());
    }

    @Test
    public void testAllDBAndOpNames() {
        List<String> names = AnnotationUtils.allDBAndOpNames();
        Assert.assertTrue(names.contains("test_fake_db_1"));
        Assert.assertTrue(names.contains("test_fake_db_2"));
        Assert.assertTrue(names.contains("test_fake_op_1"));
        Assert.assertTrue(names.contains("test_fake_op_2"));
        Assert.assertFalse(names.contains("dummy1"));
        Assert.assertFalse(names.contains("dummy2"));
    }

    @Test
    public void testCreateDB() throws Exception {
        BaseDB db1 = AnnotationUtils.createDB("test_fake_db_1", new Params());
        Assert.assertTrue(db1 instanceof FakeDB1);

        BaseDB db2 = AnnotationUtils.createDB("test_fake_db_2", new Params());
        Assert.assertTrue(db2 instanceof FakeDB2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDBError1() throws Exception {
        AnnotationUtils.createDB("A_DB_HAS_NO_NAME", new Params());
    }

    @Test
    public void testIsDB() {
        Assert.assertTrue(AnnotationUtils.isDB("test_fake_db_1"));
        Assert.assertTrue(AnnotationUtils.isDB("test_fake_db_2"));
        Assert.assertFalse(AnnotationUtils.isDB("test_fake_op_1"));
        Assert.assertFalse(AnnotationUtils.isDB("test_fake_op_2"));
        Assert.assertFalse(AnnotationUtils.isDB("dummy1"));
        Assert.assertFalse(AnnotationUtils.isDB("dummy2"));
    }

    @Test
    public void testIsDBHasTimestamp() {
        Assert.assertFalse(AnnotationUtils.isDBHasTimestamp("test_fake_db_1"));
        Assert.assertTrue(AnnotationUtils.isDBHasTimestamp("test_fake_db_2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsDBHasTimestampError() {
        Assert.assertFalse(AnnotationUtils.isDBHasTimestamp("A_DB_HAS_NO_NAME"));
    }

    @Test
    public void testIOpHasTimestamp() {
        Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_1", IOType.SourceBatch));
        Assert.assertTrue(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_2", IOType.SourceBatch));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsOpHasTimestampError() {
        Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_1", IOType.SinkStream));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsOpHasTimestampError2() {
        Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("A_DB_HAS_NO_NAME", IOType.SourceBatch));
    }

    @Test
    public void testCreateOp() throws Exception {
        AlgoOperator op1 = AnnotationUtils.createOp("test_fake_op_1", IOType.SourceBatch, new Params());
        Assert.assertTrue(op1 instanceof FakeOp1);

        AlgoOperator op2 = AnnotationUtils.createOp("test_fake_op_2", IOType.SourceBatch, new Params());
        Assert.assertTrue(op2 instanceof FakeOp2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateOpError1() throws Exception {
        AnnotationUtils.createOp("test_fake_op_1", IOType.SinkBatch, new Params());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateOpError2() throws Exception {
        AnnotationUtils.createOp("A_OP_HAS_NO_NAME", IOType.SourceBatch, new Params());
    }

}
