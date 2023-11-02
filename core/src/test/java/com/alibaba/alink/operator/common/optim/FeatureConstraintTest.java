package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FeatureConstraintTest extends AlinkTestBase {

	@Test
	public void test() {
		ConstraintBetweenBins b1 = new ConstraintBetweenBins("t1", 10);
		b1.addLargerThanBin(new Number[] {0, 1});
		b1.addScale(new Number[] {2, 3, 4.});
		b1.addEqual(new Number[] {3, 4.3});
		b1.addLessThanBin(new Number[] {4, 5, 6});
		b1.addLessThan(new Number[] {7, 8.8});
		ConstraintBetweenBins b2 = new ConstraintBetweenBins("t2", 4);
		b2.addLargerThanBin(new Number[] {0, 1});
		b2.addEqual(new Number[] {3, 4.});
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(b1, b2);
		ConstraintBetweenFeatures bfc = new ConstraintBetweenFeatures();
		bfc.addLessThanFeature("t1", 1, "t2", 0);
		System.out.println(f.toString());
		//        f.addConstraintBetweenFeature(bfc);

		String s
			= "{\"featureConstraint\":[{\"name\":\"t1\",\"dim\":10,\"UP\":[[7,8.8]],\"LO\":[],\"=\":[[3,4.3]],"
			+ "\"%\":[[2,3,4.0]],\"<\":[[4,5,6]],\">\":[[0,1]]},{\"name\":\"t2\",\"dim\":4,\"UP\":[],\"LO\":[],"
			+ "\"=\":[[3,4.0]],\"%\":[],\"<\":[],\">\":[[0,1]]}]}";
		FeatureConstraint fNew = FeatureConstraint.fromJson(s);
		Tuple4 <double[][], double[], double[][], double[]> cons = fNew.getConstraintsForFeatureWithBin();
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testFeatureTable() {
		ConstraintBetweenFeatures bfc = new ConstraintBetweenFeatures();
		bfc.addLessThanFeature("f1", 0, "f2", 0);
		HashMap <String, Integer> featureIndex = new HashMap <>(4);//here need to put all features in.
		featureIndex.put("f1", 0);
		featureIndex.put("f2", 1);
		featureIndex.put("f3", 2);
		featureIndex.put("f4", 3);
		FeatureConstraint f = new FeatureConstraint();
		System.out.println(bfc.toString());
		f.addConstraintBetweenFeature(bfc);
		Tuple4 <double[][], double[], double[][], double[]> cons = f.getConstraintsForFeatures(featureIndex);
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testParseFeatureTable() {
		HashMap <String, Integer> featureIndex = new HashMap <>(4);//here need to put all features in.
		featureIndex.put("f1", 0);
		featureIndex.put("f2", 1);
		featureIndex.put("f3", 2);
		featureIndex.put("f4", 3);
		String constraint
			= "{\"name\":\"constraintBetweenFeatures\",\"UP\":[],\"LO\":[],\"=\":[],\"%\":[],\"<\":[[\"f1\",0,\"f2\","
			+ "0,\"f3\",0]],\">\":[]}";
		ConstraintBetweenFeatures bfc = ConstraintBetweenFeatures.fromJson(constraint);
		FeatureConstraint f = new FeatureConstraint();
		f.addConstraintBetweenFeature(bfc);
		Tuple4 <double[][], double[], double[][], double[]> cons = f.getConstraintsForFeatures(featureIndex);
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testFeatureVector() {
		ConstraintBetweenFeatures bfc = new ConstraintBetweenFeatures();
		bfc.addLessThanFeature(2, 1);
		bfc.addEqual(3, -1.0);
		bfc.addScale(4, 5, 2.);
		bfc.addLargerThan(6, 9.);
		System.out.println(bfc.toString());
		FeatureConstraint f = new FeatureConstraint();
		f.addConstraintBetweenFeature(bfc);
		Tuple4 <double[][], double[], double[][], double[]> cons = f.getConstraintsForFeatures(8);
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testParseFeatureVector() {
		String constraint
			= "{\"name\":\"constraintBetweenFeatures\",\"UP\":[],\"LO\":[[6,9.0]],\"=\":[[3,-1.0]],\"%\":[[4,5,2.0]],"
			+ "\"<\":[[2,1,0]],\">\":[]}";
		ConstraintBetweenFeatures bfc = ConstraintBetweenFeatures.fromJson(constraint);
		FeatureConstraint f = new FeatureConstraint();
		f.addConstraintBetweenFeature(bfc);
		System.out.println(f.toString());
		Tuple4 <double[][], double[], double[][], double[]> cons = f.getConstraintsForFeatures(8);
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testParseFeatur() {
		String c1 = "{\"name\":\"c1\",\"LO\":[[6,9.0]],\"=\":[[3,-1.0]],\"%\":[[4,5,2.0]],\"<\":[[2,1,0]],\">\":[]}";
		ConstraintBetweenBins bfc = ConstraintBetweenBins.fromJson(c1);
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(bfc);

		String c2
			= "{\"name\":\"c2\",\"UP\":[],\"LO\":[[6,9.0]],\"=\":[[3,-1.0]],\"%\":[[4,5,2.0]],\"<\":[[2,1,0]],"
			+ "\">\":[]}";
		ConstraintBetweenBins b2 = ConstraintBetweenBins.fromJson(c2);
		f.addBinConstraint(b2);
		System.out.println(f.toString());
	}

	@Test
	public void t() {
		ConstraintBetweenBins bfc = new ConstraintBetweenBins();
		bfc.addLessThan(new Number[] {1, 2.3});
		bfc.addLessThan(new Number[] {-1, 1});
		bfc.addLessThan(new Number[] {-2, 1});
		bfc.addScale(new Number[] {1, 2, 3.4});
		bfc.addScale(new Number[] {-1, 6, 3.4});
		bfc.addScale(new Number[] {-2, 7, 3.4});
		bfc.addLargerThanBin(new Number[] {2, 3});
		bfc.addLargerThanBin(new Number[] {-1, 3});
		bfc.addLargerThanBin(new Number[] {-2, 3});
		bfc.addLessThanBin(new Number[] {3, 4});
		bfc.addLessThanBin(new Number[] {-1, 8});
		bfc.addLessThanBin(new Number[] {-2, 8});
		bfc.addEqual(new Number[] {0, 1.2});
		bfc.addEqual(new Number[] {-1, 0.2});
		bfc.addEqual(new Number[] {-2, 0.2});
		bfc.addLargerThan(new Number[] {4, 5});
		bfc.addLargerThan(new Number[] {-1, 0});
		bfc.addLargerThan(new Number[] {-2, 0});
		bfc.setName("bfc");
		FeatureConstraint f = new FeatureConstraint();
		f.addBinConstraint(bfc);
		Map <String, Boolean> map = new HashMap <>();
		map.put("bfc", true);
		f.modify(map);
	}
}
