package com.alibaba.alink.common.annotation;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.PipelineStageBase;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for operators intended for public usage.
 * <p>
 * To avoid ambiguity of names, Alink operators are divided into 2 classes: algo operators and pipelines operators.
 * <p>
 * Algo operators are further divided into batch operators and stream operators. Pipeline operators are further divided
 * into estimators and transformers.
 */
public class PublicOperatorUtils {

	final static String BASE_PKG_NAME = "com.alibaba.alink";

	final static Class <?>[] ALGO_OP_BASES = new Class[] {AlgoOperator.class};
	final static Class <?>[] PIPELINE_OP_BASES = new Class[] {PipelineStageBase.class};

	public static boolean isPublicUsable(Class <?> clazz) {
		return Modifier.isPublic(clazz.getModifiers())    // is public
			&& !Modifier.isAbstract(clazz.getModifiers())    // not abstract
			&& !clazz.isInterface()    // not an interface
			&& null == clazz.getEnclosingClass()    // not enclosed
			&& null == clazz.getAnnotation(Internal.class)    // not @Internal
			&& !ExtractModelInfoBatchOp.class.isAssignableFrom(clazz)     // not a subclass of ExtractModelInfoBatchOp
			&& !clazz.equals(Pipeline.class)  // special case
			&& !clazz.equals(PipelineModel.class)  // special case
			;
	}

	/**
	 * List all public usable operators with parent class in `bases`.
	 * <p>
	 * NOTE: this method may return different results when called in different modules.
	 *
	 * @param bases base classes
	 * @return public usable operators with parent class in `bases`
	 */
	public static List <Class <?>> listOperators(Class <?>... bases) {
		Reflections ref = new Reflections(BASE_PKG_NAME);
		List <Class <?>> operators = new ArrayList <>();
		for (Class <?> base : bases) {
			operators.addAll(ref.getSubTypesOf(base));
		}
		return operators.stream()
			.filter(PublicOperatorUtils::isPublicUsable)
			.sorted(Comparator.comparing(Class::toString))
			.collect(Collectors.toList());
	}

	public static List <Class <?>> listAlgoOperators() {
		return listOperators(ALGO_OP_BASES);
	}

	public static List <Class <?>> listPipelineOperators() {
		return listOperators(PIPELINE_OP_BASES);
	}
}
