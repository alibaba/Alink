package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AnnotationUtilsTest extends AlinkTestBase {

	@IoOpAnnotation(name = "test_fake_op_1", ioType = IOType.SourceBatch)
	private static class FakeOp1 extends FakeOpBase {
		private static final long serialVersionUID = -6521748252404002093L;

		public FakeOp1(Params params) {
			super(params);
		}
	}

	@IoOpAnnotation(name = "test_fake_op_2", ioType = IOType.SourceBatch, hasTimestamp = true)
	private static class FakeOp2 extends FakeOpBase {
		private static final long serialVersionUID = -791295941140586544L;

		public FakeOp2(Params params) {
			super(params);
		}
	}

	public static class FakeOp3 extends FakeOpBase {
		private static final long serialVersionUID = 7655071505688719672L;

		public FakeOp3(Params params) {
			super(params);
		}
	}

	@IoOpAnnotation(name = "dummy2", ioType = IOType.SourceBatch)
	private static class DummyClass2 {
	}

	@Test
	public void testAnnotatedIoType() {
		Assert.assertEquals(IOType.SourceBatch, AnnotationUtils.annotatedIoType(FakeOp1.class));
		Assert.assertEquals(IOType.SourceBatch, AnnotationUtils.annotatedIoType(FakeOp2.class));
		Assert.assertNull(AnnotationUtils.annotatedIoType(FakeOp3.class));
	}

	@Test
	public void testIOpHasTimestamp() {
		Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_1", IOType.SourceBatch));
		Assert.assertTrue(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_2", IOType.SourceBatch));
	}

	@Test(expected = AkIllegalArgumentException.class)
	public void testIsOpHasTimestampError() {
		Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("test_fake_op_1", IOType.SinkStream));
	}

	@Test(expected = AkIllegalArgumentException.class)
	public void testIsOpHasTimestampError2() {
		Assert.assertFalse(AnnotationUtils.isIoOpHasTimestamp("A_DB_HAS_NO_NAME", IOType.SourceBatch));
	}

	@Test
	public void testAnnotatedName() {
		Assert.assertEquals("test_fake_op_1", AnnotationUtils.annotatedName(FakeOp1.class));
		Assert.assertEquals("test_fake_op_2", AnnotationUtils.annotatedName(FakeOp2.class));
	}

	@Test
	public void testAllDBAndOpNames() {
		List <String> names = AnnotationUtils.allOpNames();
		Assert.assertTrue(names.contains("test_fake_op_1"));
		Assert.assertTrue(names.contains("test_fake_op_2"));
		Assert.assertFalse(names.contains("dummy2"));
	}

	@Test
	public void testCreateOp() throws Exception {
		AlgoOperator <?> op1 = AnnotationUtils.createOp("test_fake_op_1", IOType.SourceBatch, new Params());
		Assert.assertTrue(op1 instanceof FakeOp1);

		AlgoOperator <?> op2 = AnnotationUtils.createOp("test_fake_op_2", IOType.SourceBatch, new Params());
		Assert.assertTrue(op2 instanceof FakeOp2);
	}

	@Test(expected = AkIllegalArgumentException.class)
	public void testCreateOpError1() throws Exception {
		AnnotationUtils.createOp("test_fake_op_1", IOType.SinkBatch, new Params());
	}

	@Test(expected = AkIllegalArgumentException.class)
	public void testCreateOpError2() throws Exception {
		AnnotationUtils.createOp("A_OP_HAS_NO_NAME", IOType.SourceBatch, new Params());
	}

}
