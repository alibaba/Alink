package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.operator.AlgoOperator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Annotation parser.
 *
 * <p>Use reflections to get the annotation of class.
 */
public class AnnotationUtils {

	private static final Logger LOG = LoggerFactory.getLogger(AnnotationUtils.class);

	/**
	 * Create a dynamic {@link ParamInfo}
	 *
	 * @param key the key of {@link ParamInfo}.
	 * @return created {@link ParamInfo}.
	 */
	public static ParamInfo <String> dynamicParamKey(String key) {
		return ParamInfoFactory.createParamInfo(key, String.class)
			.setDescription("Key: " + key)
			.build();
	}

	static class Wrapper<T> {
		Class <? extends T> clazz;
		boolean hasTimestamp;

		Wrapper(Class <? extends T> clazz, boolean hasTimestamp) {
			this.clazz = clazz;
			this.hasTimestamp = hasTimestamp;
		}
	}

	/**
	 * All annotated FileSystem classes, annotated name as key.
	 */
	private static final Map <String, Class <? extends BaseFileSystem <?>>> FILE_SYSTEM_CLASSES
		= loadFileSystemClasses();

	/**
	 * All annotated IO operator classes, annotated name as row key, IOType as column key.
	 */
	private static final Table <String, IOType, Wrapper <AlgoOperator <?>>> IO_OP_CLASSES = loadIoOpClasses();

	@SuppressWarnings("unchecked")
	private static Map <String, Class <? extends BaseFileSystem <?>>> loadFileSystemClasses() {
		Reflections reflections = new Reflections("com.alibaba.alink");
		Map <String, Class <? extends BaseFileSystem <?>>> map = new HashMap <>();
		Set <Class <?>> set = reflections.getTypesAnnotatedWith(FSAnnotation.class);
		for (Class <?> clazz : set) {
			if (!BaseFileSystem.class.isAssignableFrom(clazz)) {
				LOG.error("DB class annotated with @DBAnnotation should be subclass of BaseDB: {}",
					clazz.getCanonicalName());
				continue;
			}

			FSAnnotation annotation = clazz.getAnnotation(FSAnnotation.class);
			String name = annotation.name();
			Class <? extends BaseFileSystem <?>> origin = map.put(name, (Class <? extends BaseFileSystem <?>>) clazz);
			if (origin != null) {
				LOG.error("Multiple DB class with same name {}: {} and {}",
					name, origin.getCanonicalName(), clazz.getCanonicalName());
			}
		}

		return ImmutableMap.copyOf(map);
	}

	@SuppressWarnings("unchecked")
	private static Table <String, IOType, Wrapper <AlgoOperator <?>>> loadIoOpClasses() {
		Reflections reflections = new Reflections("com.alibaba.alink");
		Table <String, IOType, Wrapper <AlgoOperator <?>>> table = HashBasedTable.create();
		for (Class <?> clazz : reflections.getTypesAnnotatedWith(IoOpAnnotation.class)) {
			if (!AlgoOperator.class.isAssignableFrom(clazz)) {
				LOG.error("Class annotated with @IoOpAnnotation should be subclass of AlgoOperator: {}",
					clazz.getCanonicalName());
				continue;
			}

			IoOpAnnotation annotation = clazz.getAnnotation(IoOpAnnotation.class);
			String name = annotation.name();
			IOType ioType = annotation.ioType();
			boolean hasTimestamp = annotation.hasTimestamp();

			Wrapper <AlgoOperator <?>> origin = table.put(name, ioType,
				new Wrapper <>((Class <? extends AlgoOperator <?>>) clazz, hasTimestamp));

			if (origin != null) {
				LOG.error("Multiple IO Operator class with same name {} and IOType: {}: {} and {}",
					name, ioType, origin.clazz.getCanonicalName(), clazz.getCanonicalName());
			}
		}

		return ImmutableTable.copyOf(table);
	}

	/**
	 * Get the name attribute in {@link IoOpAnnotation} of IO operator or {@link FSAnnotation} classes.
	 *
	 * @param clazz DB or IO operator class
	 * @return annotated name.
	 */
	public static String annotatedName(Class <?> clazz) {
		if (AlgoOperator.class.isAssignableFrom(clazz)) {
			IoOpAnnotation annotation = clazz.getAnnotation(IoOpAnnotation.class);
			return annotation == null ? null : annotation.name();
		} else if (BaseFileSystem.class.isAssignableFrom(clazz)) {
			FSAnnotation annotation = clazz.getAnnotation(FSAnnotation.class);
			return annotation == null ? null : annotation.name();
		} else {
			throw new IllegalStateException(
				"Only IO Operator, filesystem class have annotated name: " + clazz.getCanonicalName());
		}
	}

	/**
	 * Get the ioType attribute in {@link IoOpAnnotation} of IO operator classes .
	 *
	 * @param cls IO operator class.
	 * @return annotated ioType.
	 */
	public static IOType annotatedIoType(Class <? extends AlgoOperator> cls) {
		IoOpAnnotation annotation = cls.getAnnotation(IoOpAnnotation.class);
		return annotation == null ? null : annotation.ioType();
	}

	public static List <String> allOpNames() {
		return new ArrayList <>(IO_OP_CLASSES.rowKeySet());
	}

	/**
	 * Get all annotated names of FileSystem classes.
	 *
	 * @return name list.
	 */
	public static List <String> allFileSystemNames() {
		return new ArrayList <>(FILE_SYSTEM_CLASSES.keySet());
	}

	/**
	 * Create a FileSystem object with given name attribute. The constructor with single {@link Params} argument
	 * will be
	 * called.
	 *
	 * @param name      annotated name of FileSystem.
	 * @param parameter the param passed to constructor.
	 * @return the crete FileSystem object.
	 * @throws Exception if given name is invalid or exception thrown in constructor.
	 */
	public static BaseFileSystem <?> createFileSystem(String name, Params parameter) throws Exception {
		Class <? extends BaseFileSystem <?>> fileSystem = FILE_SYSTEM_CLASSES.get(name);
		Preconditions.checkArgument(fileSystem != null, "No FileSystem named %s", name);
		return fileSystem.getConstructor(Params.class).newInstance(parameter);
	}

	/**
	 * Get the hasTimestamp attribute of given IO operator.
	 *
	 * @param name annotated name of IO operator.
	 * @param type ioType of IO operator.
	 * @return true if it has timestamp
	 * @throws IllegalArgumentException if no IO operator matches given name and ioType.
	 */
	public static boolean isIoOpHasTimestamp(String name, IOType type) {
		Wrapper <AlgoOperator <?>> op = IO_OP_CLASSES.get(name, type);
		Preconditions.checkArgument(op != null, "No OP named %s has IOType: %s", name, type);
		return op.hasTimestamp;
	}

	/**
	 * Check if there is a FileSystem has given annotated name.
	 *
	 * @param name FileSystem name to check.
	 * @return true if there is.
	 */
	public static boolean isFileSystem(String name) {
		return FILE_SYSTEM_CLASSES.containsKey(name);
	}

	/**
	 * Create a IO operator object with given name and ioType attribute. The constructor with single {@link Params}
	 * argument will be called.
	 *
	 * @param name      annotated name of operator.
	 * @param type      ioType of operator.
	 * @param parameter the param passed to constructor.
	 * @return null if no operator matches given name and ioType, otherwise, the created operator.
	 * @throws Exception if given name and ioType is invalid or exception thrown in constructor
	 */
	public static AlgoOperator <?> createOp(String name, IOType type, Params parameter) throws Exception {
		Wrapper <AlgoOperator <?>> op = IO_OP_CLASSES.get(name, type);
		Preconditions.checkArgument(op != null, "No OP named %s has IOType: %s", name, type);
		return op.clazz.getConstructor(Params.class).newInstance(parameter);
	}
}
