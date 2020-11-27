package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.annotations.AnnotationUtils.Wrapper;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.google.common.collect.ImmutableMap;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CatalogAnnotationUtils {
	private static final Logger LOG = LoggerFactory.getLogger(CatalogAnnotationUtils.class);

	private static final Map <String, Wrapper <BaseCatalog>> CATALOG_CLASSES = loadCatalogClasses();

	private static Map <String, Wrapper <BaseCatalog>> loadCatalogClasses() {
		Reflections reflections = new Reflections("com.alibaba.alink");
		Map <String, Wrapper <BaseCatalog>> map = new HashMap <>();
		Set <Class <?>> set = reflections.getTypesAnnotatedWith(CatalogAnnotation.class);
		for (Class <?> clazz : set) {
			if (!BaseCatalog.class.isAssignableFrom(clazz)) {
				LOG.error("Catalog class annotated with @CatalogAnnotation should be subclass of BaseCatalog: {}",
					clazz.getCanonicalName());
				continue;
			}

			CatalogAnnotation annotation = clazz.getAnnotation(CatalogAnnotation.class);
			String name = annotation.name();
			boolean hasTimestamp = annotation.hasTimestamp();
			Wrapper <BaseCatalog> origin = map.put(name,
				new Wrapper <>((Class <? extends BaseCatalog>) clazz, hasTimestamp));
			if (origin != null) {
				LOG.error("Multiple catalog class with same name {}: {} and {}",
					name, origin.clazz.getCanonicalName(), clazz.getCanonicalName());
			}
		}

		return ImmutableMap.copyOf(map);
	}

	public static BaseCatalog createCatalog(String name, Params parameter) throws Exception {
		Wrapper <BaseCatalog> catalog = CATALOG_CLASSES.get(name);
		Preconditions.checkArgument(catalog != null, "No catalog named %s", name);
		return catalog.clazz.getConstructor(Params.class).newInstance(parameter);
	}

	public static boolean isCatalog(String name) {
		return CATALOG_CLASSES.containsKey(name);
	}

	public static String annotatedName(Class <?> clazz) {
		if (BaseCatalog.class.isAssignableFrom(clazz)) {
			CatalogAnnotation annotation = clazz.getAnnotation(CatalogAnnotation.class);
			return annotation == null ? null : annotation.name();
		} else {
			throw new IllegalStateException(
				"Only catalog class have annotated name: " + clazz.getCanonicalName());
		}
	}
}
