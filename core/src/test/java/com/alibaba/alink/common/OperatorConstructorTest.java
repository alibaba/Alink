package com.alibaba.alink.common;

import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OperatorConstructorTest extends AlinkTestBase {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T extends WithParams> void testConstructor(Class <T> clazz) {
		Constructor <?>[] constructors = clazz.getConstructors();
		for (Constructor <?> constructor : constructors) {
			Parameter[] parameters = constructor.getParameters();
			int nParams = parameters.length;
			T instance = null;
			try {
				if (nParams == 0) {
					instance = (T) constructor.newInstance();
				} else if ((nParams == 1) && (parameters[0].getType().equals(Params.class))) {
					Params params = new Params();
					instance = (T) constructor.newInstance(params);
				} else if ((nParams == 1) && (parameters[0].getType().equals(BatchOperator.class))) {
					BatchOperator model = new NumSeqSourceBatchOp(1);   // fake model
					instance = (T) constructor.newInstance(model);
				} else if ((nParams == 2) && (parameters[0].getType().equals(BatchOperator.class)) && (parameters[1]
					.getType().equals(Params.class))) {
					BatchOperator model = new NumSeqSourceBatchOp(1);   // fake model
					Params params = new Params();
					instance = (T) constructor.newInstance(model, params);
				} else {
					//                    System.out.println(clazz.getCanonicalName());
				}
			} catch (Exception ex) {
				Assert.fail(ex.toString());
			}
			if (null != instance) {
				Assert.assertNotNull(instance.getParams());
			}
		}
	}

	private static final Set <Class <?>> IGNORED_CLASSED = new HashSet <>(Arrays. <Class <?>>asList(
		FtrlPredictStreamOp.class, FtrlTrainStreamOp.class
	));

	@SuppressWarnings("rawtypes")
	public void testConstructorsOfSubtype(Class <? extends WithParams> base) {
		Reflections reflections = new Reflections("com.alibaba.alink");
		List <Class <? extends WithParams>> classes =
			reflections.getSubTypesOf(base).stream()
				.filter(d -> !IGNORED_CLASSED.contains(d))
				.filter(
					d -> !d.isInterface()
						&& !Modifier.isAbstract(d.getModifiers())
						&& Modifier.isPublic(d.getModifiers())
						&& !d.isMemberClass()
				)
				.filter(d -> reflections.getSubTypesOf(d).size() == 0)
				.sorted((o1, o2) -> new StringComparator(true).compare(o1.getCanonicalName(), o2.getCanonicalName()))
				.collect(Collectors.toList());
		for (Class <? extends WithParams> clazz : classes) {
			long start = System.currentTimeMillis();
			testConstructor(clazz);
			long end = System.currentTimeMillis();
			if (end - start > 100) {
				// print classes with test time > 100 ms
				System.out.println(String.format("%s: %d", clazz.getCanonicalName(), end - start));
			}
		}
	}

	@Test
	public void testConstructors() {
		testConstructorsOfSubtype(AlgoOperator.class);
		testConstructorsOfSubtype(PipelineStageBase.class);
	}
}
