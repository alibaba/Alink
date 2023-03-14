package com.alibaba.alink.common.annotation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.WithParams;

import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class ParamAnnotationUtils {
	final static String BASE_PARAMS_PKG_NAME = "com.alibaba.alink.params";
	final static Class <?>[] PARAM_BASES = new Class[] {WithParams.class};

	static List <Class <?>> getAllInterfaces(Class <?> clz) {
		Set <Class <?>> visited = new HashSet <>();
		Queue <Class <?>> classes = new LinkedBlockingDeque <>();
		classes.add(clz);
		while (!classes.isEmpty()) {
			Class <?> c = classes.poll();
			if (visited.contains(c)) {
				continue;
			}
			visited.add(c);
			classes.addAll(Arrays.asList(c.getInterfaces()));
		}
		return new ArrayList <>(visited);
	}

	public static List <ParamSelectColumnSpec> getParamSelectColumnSpecs(Class <?> clz) {
		List <ParamSelectColumnSpec> validSpecs = new ArrayList <>();
		List <Class <?>> interfaces = getAllInterfaces(clz);
		Queue <Annotation> q = new LinkedBlockingDeque <>();
		for (Class <?> anInterface : interfaces) {
			q.addAll(Arrays.asList(anInterface.getAnnotations()));
		}
		// If a same parameters was specified more than once, only use the first encountered one
		Set <String> names = new HashSet <>();
		while (!q.isEmpty()) {
			Annotation annotation = q.poll();
			Class <? extends Annotation> annotationType = annotation.annotationType();
			if (annotationType.getName().startsWith("java.lang.")) {
				continue;
			}
			if (annotationType.equals(ParamSelectColumnSpec.class)) {
				ParamSelectColumnSpec spec = (ParamSelectColumnSpec) annotation;
				if (!names.contains(spec.name())) {
					validSpecs.add(spec);
					names.add(spec.name());
				}
			}
			if (annotationType.equals(ParamSelectColumnSpecs.class)) {
				ParamSelectColumnSpecs specs = (ParamSelectColumnSpecs) annotation;
				for (ParamSelectColumnSpec spec : specs.value()) {
					if (!names.contains(spec.name())) {
						validSpecs.add(spec);
						names.add(spec.name());
					}
				}
			} else {
				q.addAll(Arrays.asList(annotationType.getAnnotations()));
			}
		}
		return validSpecs;
	}

	public static List <ParamMutexRule> getParamMutexRules(Class <?> clz) {
		List <ParamMutexRule> specs = new ArrayList <>();
		List <Class <?>> interfaces = getAllInterfaces(clz);
		Queue <Annotation> q = new LinkedBlockingDeque <>();
		for (Class <?> anInterface : interfaces) {
			q.addAll(Arrays.asList(anInterface.getAnnotations()));
		}
		while (!q.isEmpty()) {
			Annotation annotation = q.poll();
			Class <? extends Annotation> annotationType = annotation.annotationType();
			if (annotationType.getName().startsWith("java.lang.")) {
				continue;
			}
			if (annotationType.equals(ParamMutexRule.class)) {
				specs.add((ParamMutexRule) annotation);
			}
			if (annotationType.equals(ParamMutexRules.class)) {
				specs.addAll(Arrays.asList(((ParamMutexRules) annotation).value()));
			} else {
				q.addAll(Arrays.asList(annotationType.getAnnotations()));
			}
		}
		return specs;
	}

	public static List <ParamsIgnoredOnWebUI> getParamsIgnoredOnUI(Class <?> clz) {
		List <ParamsIgnoredOnWebUI> specs = new ArrayList <>();
		List <Class <?>> interfaces = getAllInterfaces(clz);
		Queue <Annotation> q = new LinkedBlockingDeque <>();
		for (Class <?> anInterface : interfaces) {
			q.addAll(Arrays.asList(anInterface.getAnnotations()));
		}
		while (!q.isEmpty()) {
			Annotation annotation = q.poll();
			Class <? extends Annotation> annotationType = annotation.annotationType();
			if (annotationType.getName().startsWith("java.lang.")) {
				continue;
			}
			if (annotationType.equals(ParamsIgnoredOnWebUI.class)) {
				specs.add((ParamsIgnoredOnWebUI) annotation);
			}
			q.addAll(Arrays.asList(annotationType.getAnnotations()));
		}
		return specs;
	}

	public static HashSet <TypeInformation <?>> getAllowedTypes(ParamSelectColumnSpec spec) {
		HashSet <TypeInformation <?>> s = new HashSet <>();
		for (TypeCollections typeCollection : spec.allowedTypeCollections()) {
			s.addAll(Arrays.asList(typeCollection.getTypes()));
		}
		return s;
	}

	public static List <Class <?>> listParamInfos(Class <?>... bases) {
		Reflections ref = new Reflections(BASE_PARAMS_PKG_NAME);
		List <Class <?>> params = new ArrayList <>();
		for (Class <?> base : bases) {
			params.addAll(ref.getSubTypesOf(base));
		}
		return params.stream()
			.filter(PublicOperatorUtils::isPublicUsable)
			.sorted(Comparator.comparing(Class::toString))
			.collect(Collectors.toList());
	}

}
