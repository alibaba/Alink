package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.operator.AlgoOperator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import org.apache.flink.util.Preconditions;
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

    private static class Wrapper<T> {
        Class<? extends T> clazz;
        boolean hasTimestamp;

        Wrapper(Class<? extends T> clazz, boolean hasTimestamp) {
            this.clazz = clazz;
            this.hasTimestamp = hasTimestamp;
        }
    }

    /**
     * All annotated DB classes, annotated name as key.
     */
    private static final Map<String, Wrapper<BaseDB>> DB_CLASSES = loadDBClasses();

    /**
     * All annotated IO operator classes, annotated name as row key, IOType as column key.
     */
    private static final Table<String, IOType, Wrapper<AlgoOperator>> IO_OP_CLASSES = loadIoOpClasses();

    @SuppressWarnings("unchecked")
    private static Map<String, Wrapper<BaseDB>> loadDBClasses() {
        Reflections reflections = new Reflections("com.alibaba.alink");
        Map<String, Wrapper<BaseDB>> map = new HashMap<>();
        Set<Class<?>> set = reflections.getTypesAnnotatedWith(DBAnnotation.class);
        for (Class<?> clazz : set) {
            if (!BaseDB.class.isAssignableFrom(clazz)) {
                LOG.error("DB class annotated with @DBAnnotation should be subclass of BaseDB: {}",
                        clazz.getCanonicalName());
                continue;
            }

            DBAnnotation annotation = clazz.getAnnotation(DBAnnotation.class);
            String name = annotation.name();
            boolean hasTimestamp = annotation.hasTimestamp();
            Wrapper<BaseDB> origin = map.put(name, new Wrapper<>((Class<? extends BaseDB>) clazz, hasTimestamp));
            if (origin != null) {
                LOG.error("Multiple DB class with same name {}: {} and {}",
                        name, origin.clazz.getCanonicalName(), clazz.getCanonicalName());
            }
        }

        return ImmutableMap.copyOf(map);
    }

    @SuppressWarnings("unchecked")
    private static Table<String, IOType, Wrapper<AlgoOperator>> loadIoOpClasses() {
        Reflections reflections = new Reflections("com.alibaba.alink");
        Table<String, IOType, Wrapper<AlgoOperator>> table = HashBasedTable.create();
        for (Class<?> clazz : reflections.getTypesAnnotatedWith(IoOpAnnotation.class)) {
            if (!AlgoOperator.class.isAssignableFrom(clazz)) {
                LOG.error("Class annotated with @IoOpAnnotation should be subclass of AlgoOperator: {}",
                        clazz.getCanonicalName());
                continue;
            }

            IoOpAnnotation annotation = clazz.getAnnotation(IoOpAnnotation.class);
            String name = annotation.name();
            IOType ioType = annotation.ioType();
            boolean hasTimestamp = annotation.hasTimestamp();

            Wrapper<AlgoOperator> origin = table.put(name, ioType,
                    new Wrapper<>((Class<? extends AlgoOperator>) clazz, hasTimestamp));


            if (origin != null) {
                LOG.error("Multiple IO Operator class with same name {} and IOType: {}: {} and {}",
                        name, ioType, origin.clazz.getCanonicalName(), clazz.getCanonicalName());
            }
        }

        return ImmutableTable.copyOf(table);
    }

    /**
     * Get the name attribute in {@link DBAnnotation} of DB classes or that in {@link IoOpAnnotation}
     * of IO operator classes.
     *
     * @param clazz DB or IO operator class
     * @return annotated name.
     */
    public static String annotatedName(Class<?> clazz) {
        if (AlgoOperator.class.isAssignableFrom(clazz)) {
            IoOpAnnotation annotation = clazz.getAnnotation(IoOpAnnotation.class);
            return annotation == null ? null : annotation.name();
        } else if (BaseDB.class.isAssignableFrom(clazz)) {
            DBAnnotation annotation = clazz.getAnnotation(DBAnnotation.class);
            return annotation == null ? null : annotation.name();
        } else {
            throw new IllegalStateException(
                    "Only DB and IO Operator class have annotated name: " + clazz.getCanonicalName());
        }
    }

    /**
     * Get the tableNameAlias attribute in {@link DBAnnotation} of DB classes .
     *
     * @param cls DB class
     * @return annotated tableNameAlias.
     */
    public static String annotatedAlias(Class<? extends BaseDB> cls) {
        DBAnnotation annotation = cls.getAnnotation(DBAnnotation.class);
        return annotation == null ? null : annotation.tableNameAlias();
    }

    /**
     * Get the tableNameAlias of given DB.
     *
     * @param name annotated name of DB.
     * @return tableNameAlias.
     * @throws IllegalArgumentException if no DB named as given name.
     */
    public static String annotatedAlias(String name) {
        Wrapper<BaseDB> db = DB_CLASSES.get(name);
        Preconditions.checkArgument(db != null, "No DB named %s", name);
        return db.clazz.getAnnotation(DBAnnotation.class).tableNameAlias();
    }

    /**
     * Get the ioType attribute in {@link IoOpAnnotation} of IO operator classes .
     *
     * @param cls IO operator class.
     * @return annotated ioType.
     */
    public static IOType annotatedIoType(Class<? extends AlgoOperator> cls) {
        IoOpAnnotation annotation = cls.getAnnotation(IoOpAnnotation.class);
        return annotation == null ? null : annotation.ioType();
    }

    /**
     * Create a dynamic {@link ParamInfo} named as "DBKey: ${tableNameAlias}" from a DB class with given annotated name.
     *
     * @param name the DB class's annotated name.
     * @return created {@link ParamInfo}.
     */
    public static ParamInfo<String> tableAliasParamKey(String name) {
        Wrapper<BaseDB> db = DB_CLASSES.get(name);
        Preconditions.checkState(db != null, "No DB named %s", name);
        return tableAliasParamKey(db.clazz);
    }

    /**
     * Create a dynamic {@link ParamInfo} named as "DBKey: ${tableNameAlias}" from as DB class.
     *
     * @param dbClazz the DB class.
     * @return created {@link ParamInfo}.
     */
    public static ParamInfo<String> tableAliasParamKey(Class<? extends BaseDB> dbClazz) {
        DBAnnotation annotation = dbClazz.getAnnotation(DBAnnotation.class);
        Preconditions.checkState(annotation != null,
                "DB is not annotated with @DBAnnotation: %s", dbClazz.getCanonicalName());
        return dynamicParamKey(annotation.tableNameAlias());
    }

    /**
     * Create a dynamic {@link ParamInfo} named as "DBKey: ${tableNameAlias}"
     *
     * @param key tableNameAlias as key
     * @return created {@link ParamInfo}.
     */
    public static ParamInfo<String> dynamicParamKey(String key) {
        return ParamInfoFactory.createParamInfo(key, String.class)
                .setDescription("DbKey: " + key)
                .build();
    }

    /**
     * Get all annotated names of DB and IO operator classes.
     *
     * @return name list.
     */
    public static List<String> allDBAndOpNames() {
        List<String> result = new ArrayList<>();
        result.addAll(DB_CLASSES.keySet());
        result.addAll(IO_OP_CLASSES.rowKeySet());
        return result;
    }


    /**
     * Create a DB object with given name attribute. The constructor with single {@link Params} argument will be called.
     *
     * @param name      annotated name of db.
     * @param parameter the param passed to constructor.
     * @return the crete DB object.
     * @throws Exception if given name is invalid or exception thrown in constructor.
     */
    public static BaseDB createDB(String name, Params parameter) throws Exception {
        Wrapper<BaseDB> db = DB_CLASSES.get(name);
        Preconditions.checkArgument(db != null, "No DB named %s", name);
        return db.clazz.getConstructor(Params.class).newInstance(parameter);
    }

    /**
     * Check if there is a DB has given annotated name.
     *
     * @param name DB name to check.
     * @return true if there is.
     */
    public static boolean isDB(String name) {
        return DB_CLASSES.containsKey(name);
    }

    /**
     * Get the hasTimestamp attribute of given DB.
     *
     * @param name annotated name of DB.
     * @return true if it has timestamp
     * @throws IllegalArgumentException if no DB named as given name.
     */
    public static boolean isDBHasTimestamp(String name) {
        Wrapper<BaseDB> db = DB_CLASSES.get(name);
        Preconditions.checkArgument(db != null, "No DB named %s", name);
        return db.clazz.getAnnotation(DBAnnotation.class).hasTimestamp();
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
        Wrapper<AlgoOperator> op = IO_OP_CLASSES.get(name, type);
        Preconditions.checkArgument(op != null, "No OP named %s has IOType: %s", name, type);
        return op.hasTimestamp;
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
    public static AlgoOperator createOp(String name, IOType type, Params parameter) throws Exception {
        Wrapper<AlgoOperator> op = IO_OP_CLASSES.get(name, type);
        Preconditions.checkArgument(op != null, "No OP named %s has IOType: %s", name, type);
        return op.clazz.getConstructor(Params.class).newInstance(parameter);
    }
}
