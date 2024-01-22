package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkColumnNotFoundException;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.viz.DataTypeDisplayInterface;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.similarity.similarity.LevenshteinSimilarity;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasLabelColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Utility to operator to interact with Table contents, such as rows and columns.
 */
public class TableUtil {
	public static final BiMap <String, TypeInformation <?>> STRING_TYPE_MAP = HashBiMap.create();
	public static final Set <String> STRING_TYPE_SET = new HashSet <>();
	private static final LevenshteinSimilarity levenshteinSimilarity = new LevenshteinSimilarity();
	public static final int DISPLAY_SIZE = 6;
	public static final String HEX = "0123456789abcdef";

	static {
		STRING_TYPE_MAP.put("STRING", Types.STRING);
		STRING_TYPE_MAP.put("VARBINARY", AlinkTypes.VARBINARY);
		STRING_TYPE_MAP.put("VECTOR", AlinkTypes.VECTOR);
		STRING_TYPE_MAP.put("DENSE_VECTOR", AlinkTypes.DENSE_VECTOR);
		STRING_TYPE_MAP.put("SPARSE_VECTOR", AlinkTypes.SPARSE_VECTOR);
		STRING_TYPE_MAP.put("TENSOR", AlinkTypes.TENSOR);
		STRING_TYPE_MAP.put("BOOL_TENSOR", AlinkTypes.BOOL_TENSOR);
		STRING_TYPE_MAP.put("BYTE_TENSOR", AlinkTypes.BYTE_TENSOR);
		STRING_TYPE_MAP.put("DOUBLE_TENSOR", AlinkTypes.DOUBLE_TENSOR);
		STRING_TYPE_MAP.put("FLOAT_TENSOR", AlinkTypes.FLOAT_TENSOR);
		STRING_TYPE_MAP.put("INT_TENSOR", AlinkTypes.INT_TENSOR);
		STRING_TYPE_MAP.put("LONG_TENSOR", AlinkTypes.LONG_TENSOR);
		STRING_TYPE_MAP.put("STRING_TENSOR", AlinkTypes.STRING_TENSOR);
		STRING_TYPE_MAP.put("MTABLE", AlinkTypes.M_TABLE);

		STRING_TYPE_SET.add("VEC_TYPES_VECTOR");
		STRING_TYPE_SET.add("VEC_TYPES_DENSE_VECTOR");
		STRING_TYPE_SET.add("VEC_TYPES_SPARSE_VECTOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_BOOL_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_BYTE_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_DOUBLE_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_FLOAT_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_INT_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_LONG_TENSOR");
		STRING_TYPE_SET.add("TENSOR_TYPES_STRING_TENSOR");
	}

	/**
	 * Return a temp table named with prefix `temp_`, follow by a random UUID.
	 *
	 * <p>UUID hyphens ("-") will be replaced by underscores ("_").
	 *
	 * @return tableName
	 */
	public static synchronized String getTempTableName() {
		return getTempTableName("temp_");
	}

	/**
	 * Return a temp table named with prefix, follow by a random UUID.
	 *
	 * <p>UUID hyphens ("-") will be replaced by underscores ("_").
	 *
	 * @return tableName
	 */
	public static synchronized String getTempTableName(String prefix) {
		return (prefix + UUID.randomUUID().toString().replaceAll("-", "_")).toLowerCase();
	}

	/**
	 * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. It will ignore the case of the
	 * tableCols.
	 *
	 * @param tableCols a string array among which to find the targetCol.
	 * @param targetCol the targetCol to find.
	 * @return the index of the targetCol, if not found, returns -1.
	 */
	public static int findColIndex(String[] tableCols, String targetCol) {
		AkPreconditions.checkNotNull(targetCol, "targetCol is null!");
		for (int i = 0; i < tableCols.length; i++) {
			if (targetCol.equalsIgnoreCase(tableCols[i])) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. If not found, it will throw a
	 * runtime exception.
	 *
	 * @param tableCols a string array among which to find the targetCol.
	 * @param targetCol the targetCol to find.
	 * @return the index of the targetCol, if not found, returns -1.
	 */
	public static int findColIndexWithAssert(String[] tableCols, String targetCol) {
		int index = findColIndex(tableCols, targetCol);
		if (index < 0) {
			throw new AkColumnNotFoundException("Can not find column: " + targetCol);
		}
		return index;
	}

	/**
	 * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. If not found, it will try to
	 * find the most similar cols, and throw a runtime exception include colname hint.
	 *
	 * @param tableCols a string array among which to find the targetCol.
	 * @param targetCol the targetCol to find.
	 * @return the index of the targetCol, if not found, returns -1.
	 */
	public static int findColIndexWithAssertAndHint(String[] tableCols, String targetCol) {
		int index = findColIndex(tableCols, targetCol);
		if (index < 0) {
			double maxSimilarity = 0.0;
			String similarCol = null;
			for (String s : tableCols) {
				double similarity = levenshteinSimilarity.calc(s, targetCol);
				if (similarity > maxSimilarity) {
					maxSimilarity = similarity;
					similarCol = s;
				}
			}
			if (maxSimilarity > 0.7) {
				throw new AkColumnNotFoundException(
					"Can not find column: " + targetCol + ", do you mean: " + similarCol + " ?");
			} else {
				throw new AkColumnNotFoundException("Can not find column: " + targetCol);
			}
		}
		return index;
	}

	/**
	 * Find the index of <code>targetCol</code> from the <code>tableSchema</code>.
	 *
	 * @param tableSchema the TableSchema among which to find the targetCol.
	 * @param targetCol   the targetCols to find.
	 * @return the index of the targetCol.
	 */
	public static int findColIndex(TableSchema tableSchema, String targetCol) {
		return findColIndex(tableSchema.getFieldNames(), targetCol);
	}

	/**
	 * Find the index of <code>targetCol</code> from the <code>tableSchema</code>. If not found, it will throw an
	 * exception.
	 *
	 * @param tableSchema the TableSchema among which to find the targetCol.
	 * @param targetCol   the targetCols to find.
	 * @return the index of the targetCol.
	 */
	public static int findColIndexWithAssert(TableSchema tableSchema, String targetCol) {
		return findColIndexWithAssert(tableSchema.getFieldNames(), targetCol);
	}

	public static int findColIndexWithAssertAndHint(TableSchema tableSchema, String targetCol) {
		return findColIndexWithAssertAndHint(tableSchema.getFieldNames(), targetCol);
	}

	/**
	 * Find the indices of <code>targetCols</code> in string array <code>tableCols</code>. If
	 * <code>targetCols</code> is
	 * null, it will be replaced by the <code>tableCols</code>
	 *
	 * @param tableCols  a string array among which to find the targetCols.
	 * @param targetCols the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndices(String[] tableCols, String[] targetCols) {
		if (targetCols == null) {
			int[] indices = new int[tableCols.length];
			for (int i = 0; i < tableCols.length; i++) {
				indices[i] = i;
			}
			return indices;
		}
		int[] indices = new int[targetCols.length];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = findColIndex(tableCols, targetCols[i]);
		}
		return indices;
	}

	/**
	 * Find the indices of <code>targetCols</code> in string array <code>tableCols</code>. If
	 * <code>targetCols</code> is
	 * null, it will be replaced by the <code>tableCols</code>. If not found, it will throw and exception.
	 *
	 * @param tableCols  a string array among which to find the targetCols.
	 * @param targetCols the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndicesWithAssert(String[] tableCols, String[] targetCols) {
		if (targetCols == null) {
			int[] indices = new int[tableCols.length];
			for (int i = 0; i < tableCols.length; i++) {
				indices[i] = i;
			}
			return indices;
		}
		int[] indices = new int[targetCols.length];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = findColIndexWithAssert(tableCols, targetCols[i]);
		}
		return indices;
	}

	/**
	 * Find the indices of <code>targetCols</code> in string array <code>tableCols</code>. If
	 * <code>targetCols</code> is
	 * null, it will be replaced by the <code>tableCols</code>. If not found, it will throw and exception.
	 *
	 * @param tableCols  a string array among which to find the targetCols.
	 * @param targetCols the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndicesWithAssertAndHint(String[] tableCols, String[] targetCols) {
		if (targetCols == null) {
			int[] indices = new int[tableCols.length];
			for (int i = 0; i < tableCols.length; i++) {
				indices[i] = i;
			}
			return indices;
		}
		int[] indices = new int[targetCols.length];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = findColIndexWithAssertAndHint(tableCols, targetCols[i]);
		}
		return indices;
	}

	/**
	 * Find the indices of <code>targetCols</code> from the <code>tableSchema</code>.
	 *
	 * @param tableSchema the TableSchema among which to find the targetCols.
	 * @param targetCols  the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndices(TableSchema tableSchema, String[] targetCols) {
		return findColIndices(tableSchema.getFieldNames(), targetCols);
	}

	/**
	 * Find the indices of <code>targetCols</code> from the <code>tableSchema</code>. If not found, it will throw an
	 * exception.
	 *
	 * @param tableSchema the TableSchema among which to find the targetCols.
	 * @param targetCols  the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndicesWithAssert(TableSchema tableSchema, String[] targetCols) {
		return findColIndicesWithAssert(tableSchema.getFieldNames(), targetCols);
	}

	/**
	 * Find the indices of <code>targetCols</code> from the <code>tableSchema</code>. If not found, it will throw an
	 * exception.
	 *
	 * @param tableSchema the TableSchema among which to find the targetCols.
	 * @param targetCols  the targetCols to find.
	 * @return the indices of the targetCols.
	 */
	public static int[] findColIndicesWithAssertAndHint(TableSchema tableSchema, String[] targetCols) {
		return findColIndicesWithAssertAndHint(tableSchema.getFieldNames(), targetCols);
	}

	/**
	 * Find the types of the <code>targetCols</code>. If the targetCol not exist, return null.
	 *
	 * @param tableSchema TableSchema.
	 * @param targetCols  the targetCols to find.
	 * @return the corresponding types.
	 */
	public static TypeInformation <?>[] findColTypes(TableSchema tableSchema, String[] targetCols) {
		if (targetCols == null) {
			return tableSchema.getFieldTypes();
		}
		TypeInformation <?>[] types = new TypeInformation[targetCols.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = findColType(tableSchema, targetCols[i]);
		}
		return types;
	}

	/**
	 * Find the types of the <code>targetCols</code>. If the targetCol not exist, throw an exception.
	 *
	 * @param tableSchema TableSchema.
	 * @param targetCols  the targetCols to find.
	 * @return the corresponding types.
	 */
	public static TypeInformation <?>[] findColTypesWithAssert(TableSchema tableSchema, String[] targetCols) {
		if (targetCols == null) {
			return tableSchema.getFieldTypes();
		}
		TypeInformation <?>[] types = new TypeInformation[targetCols.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = findColTypeWithAssert(tableSchema, targetCols[i]);
		}
		return types;
	}

	/**
	 * Find the types of the <code>targetCols</code>. If the targetCol not exist, throw an exception.
	 *
	 * @param tableSchema TableSchema.
	 * @param targetCols  the targetCols to find.
	 * @return the corresponding types.
	 */
	public static TypeInformation <?>[] findColTypesWithAssertAndHint(TableSchema tableSchema, String[] targetCols) {
		if (targetCols == null) {
			return tableSchema.getFieldTypes();
		}
		TypeInformation <?>[] types = new TypeInformation[targetCols.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = findColTypeWithAssertAndHint(tableSchema, targetCols[i]);
		}
		return types;
	}

	/**
	 * Find the type of the <code>targetCol</code>. If the targetCol not exist, return null.
	 *
	 * @param tableSchema TableSchema
	 * @param targetCol   the targetCol to find.
	 * @return the corresponding type.
	 */
	public static TypeInformation <?> findColType(TableSchema tableSchema, String targetCol) {
		int index = findColIndex(tableSchema.getFieldNames(), targetCol);

		return index == -1 ? null : tableSchema.getFieldTypes()[index];
	}

	/**
	 * Find the type of the <code>targetCol</code>. If the targetCol not exist, throw an exception.
	 *
	 * @param tableSchema TableSchema
	 * @param targetCol   the targetCol to find.
	 * @return the corresponding type.
	 */
	public static TypeInformation <?> findColTypeWithAssert(TableSchema tableSchema, String targetCol) {
		return tableSchema.getFieldTypes()[findColIndexWithAssert(tableSchema, targetCol)];
	}

	/**
	 * Find the type of the <code>targetCol</code>. If the targetCol not exist, throw an exception.
	 *
	 * @param tableSchema TableSchema
	 * @param targetCol   the targetCol to find.
	 * @return the corresponding type.
	 */
	public static TypeInformation <?> findColTypeWithAssertAndHint(TableSchema tableSchema, String targetCol) {
		return tableSchema.getFieldTypes()[findColIndexWithAssertAndHint(tableSchema, targetCol)];
	}

	/**
	 * Determine whether it is number type, number type includes double, long, byte, int, float and short.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is number type
	 */
	public static boolean isSupportedNumericType(TypeInformation <?> dataType) {
		return Types.DOUBLE.equals(dataType)
			|| Types.LONG.equals(dataType)
			|| Types.BYTE.equals(dataType)
			|| Types.INT.equals(dataType)
			|| Types.FLOAT.equals(dataType)
			|| Types.SHORT.equals(dataType)
			|| Types.BIG_DEC.equals(dataType);
	}

	/**
	 * Determine whether it is date type.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is date type.
	 */
	public static boolean isSupportedDateType(TypeInformation <?> dataType) {
		return Types.SQL_DATE.equals(dataType)
			|| Types.SQL_TIMESTAMP.equals(dataType)
			|| Types.SQL_TIME.equals(dataType);
	}

	/**
	 * Determine whether it is boolean type.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is boolean type.
	 */
	public static boolean isSupportedBoolType(TypeInformation <?> dataType) {
		return Types.BOOLEAN.equals(dataType);
	}

	/**
	 * Determine whether it is a string type.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is string type
	 */
	public static boolean isString(TypeInformation <?> dataType) {
		return Types.STRING == dataType;
	}

	/**
	 * Determine whether it is a vector type.
	 *
	 * @param dataType the dataType to determine.
	 * @return whether it is vector type
	 */
	public static boolean isVector(TypeInformation <?> dataType) {
		return AlinkTypes.VECTOR.equals(dataType)
			|| AlinkTypes.DENSE_VECTOR.equals(dataType)
			|| AlinkTypes.SPARSE_VECTOR.equals(dataType)
			|| Types.STRING.equals(dataType)
			;
	}

	/**
	 * Check whether <code>selectedCols</code> exist or not, if not exist, throw exception.
	 *
	 * @param tableCols    a string array among which to find the target selectedCols.
	 * @param selectedCols the selectedCols to assert.
	 */
	public static void assertSelectedColExist(String[] tableCols, String... selectedCols) {
		if (null != selectedCols) {
			for (String selectedCol : selectedCols) {
				if (null != selectedCol) {
					findColIndexWithAssert(tableCols, selectedCol);
				}
			}
		}
	}

	/**
	 * Check whether colTypes of the <code>selectedCols</code> is numerical, if not, throw exception.
	 *
	 * @param tableSchema  TableSchema
	 * @param selectedCols the selectedCols to assert.
	 */
	public static void assertNumericalCols(TableSchema tableSchema, String... selectedCols) {
		if (selectedCols != null && selectedCols.length != 0) {
			for (String selectedCol : selectedCols) {
				if (null != selectedCol) {
					if (!isSupportedNumericType(findColType(tableSchema, selectedCol))) {
						throw new AkIllegalOperatorParameterException("col type must be number " + selectedCol);
					}
				}
			}
		}
	}

	/**
	 * Check whether colTypes of the <code>selectedCols</code> is string, if not, throw exception.
	 *
	 * @param tableSchema  TableSchema
	 * @param selectedCols the selectedCol to assert.
	 */
	public static void assertStringCols(TableSchema tableSchema, String... selectedCols) {
		if (selectedCols != null && selectedCols.length != 0) {
			for (String selectedCol : selectedCols) {
				if (null != selectedCol) {
					if (!isString(findColType(tableSchema, selectedCol))) {
						throw new AkIllegalOperatorParameterException("col type must be string " + selectedCol);
					}
				}
			}
		}
	}

	/**
	 * Check whether colTypes of the <code>selectedCols</code> is vector, if not, throw exception.
	 *
	 * @param tableSchema  TableSchema
	 * @param selectedCols the selectedCol to assert.
	 * @see #isVector(TypeInformation)
	 */
	public static void assertVectorCols(TableSchema tableSchema, String... selectedCols) {
		if (selectedCols != null && selectedCols.length != 0) {
			for (String selectedCol : selectedCols) {
				if (null != selectedCol) {
					if (!isVector(findColType(tableSchema, selectedCol))) {
						throw new AkIllegalOperatorParameterException("col type must be string " + selectedCol);
					}
				}
			}
		}
	}

	/**
	 * Return the columns in the table whose types are string.
	 *
	 * @param tableSchema TableSchema
	 * @return String columns.
	 */
	public static String[] getStringCols(TableSchema tableSchema) {
		return getStringCols(tableSchema, null);
	}

	/**
	 * Return the columns in the table whose types are string and are not included in the excludeCols.
	 * <p>
	 * <p>If <code>excludeCols</code> is null, return all the string columns.
	 *
	 * @param tableSchema TableSchema.
	 * @param excludeCols The columns who are not considered.
	 * @return string columns.
	 */
	public static String[] getStringCols(TableSchema tableSchema, String[] excludeCols) {
		ArrayList <String> numericCols = new ArrayList <>();
		List <String> excludeColsList = null == excludeCols ? null : Arrays.asList(excludeCols);
		String[] inColNames = tableSchema.getFieldNames();
		TypeInformation <?>[] inColTypes = tableSchema.getFieldTypes();

		for (int i = 0; i < inColNames.length; i++) {
			if (isString(inColTypes[i])) {
				if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
					numericCols.add(inColNames[i]);
				}
			}
		}

		return numericCols.toArray(new String[0]);
	}

	/**
	 * Return the columns in the table whose types are numeric.
	 *
	 * @param tableSchema TableSchema
	 * @return numeric columns.
	 */
	public static String[] getNumericCols(TableSchema tableSchema) {
		return getNumericCols(tableSchema, null);
	}

	/**
	 * Return the columns in the table whose types are numeric and are not included in the excludeCols.
	 * <p>
	 * <p>If <code>excludeCols</code> is null, return all the numeric columns.
	 *
	 * @param tableSchema TableSchema.
	 * @param excludeCols the columns who are not considered.
	 * @return numeric columns.
	 */
	public static String[] getNumericCols(TableSchema tableSchema, String[] excludeCols) {
		ArrayList <String> numericCols = new ArrayList <>();
		List <String> excludeColsList = (null == excludeCols ? null : Arrays.asList(excludeCols));
		String[] inColNames = tableSchema.getFieldNames();
		TypeInformation <?>[] inColTypes = tableSchema.getFieldTypes();

		for (int i = 0; i < inColNames.length; i++) {
			if (isSupportedNumericType(inColTypes[i])) {
				if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
					numericCols.add(inColNames[i]);
				}
			}
		}

		return numericCols.toArray(new String[0]);
	}

	/**
	 * Get the columns from featureCols who are included in the <code>categoricalCols</code>, and the columns whose
	 * types are string or boolean.
	 * <p>If <code>categoricalCols</code> is null, return all the categorical columns.
	 *
	 * <p>for example: In FeatureHasher which projects a number of categorical or numerical features
	 * into a feature vector of a specified dimension needs to identify the categorical features. And the column which
	 * is the string or boolean must be categorical. We need to find these columns as categorical when user do not
	 * specify the types(categorical or numerical).
	 *
	 * @param tableSchema     TableSchema.
	 * @param featureCols     the columns to chosen from.
	 * @param categoricalCols the columns which are included in the final result whatever the types of them are. And it
	 *                        must be a subset of featureCols.
	 * @return the categoricalCols.
	 */
	public static String[] getCategoricalCols(
		TableSchema tableSchema, String[] featureCols, String[] categoricalCols) {
		if (null == featureCols) {
			return categoricalCols;
		}
		List <String> categoricalList = null == categoricalCols ? null : Arrays.asList(categoricalCols);
		List <String> featureList = Arrays.asList(featureCols);
		if (null != categoricalCols && !featureList.containsAll(categoricalList)) {
			throw new AkIllegalArgumentException("CategoricalCols must be included in featureCols!");
		}

		TypeInformation <?>[] featureColTypes = findColTypes(tableSchema, featureCols);
		List <String> res = new ArrayList <>();
		for (int i = 0; i < featureCols.length; i++) {
			boolean included = null != categoricalList && categoricalList.contains(featureCols[i]);
			if (included || Types.BOOLEAN == featureColTypes[i] || Types.STRING == featureColTypes[i]) {
				res.add(featureCols[i]);
			}
		}

		return res.toArray(new String[0]);
	}

	public static String[] getOptionalFeatureCols(TableSchema tableSchema, Params params) {
		if (params.contains(HasFeatureColsDefaultAsNull.FEATURE_COLS)) {
			return params.get(HasFeatureColsDefaultAsNull.FEATURE_COLS);
		}

		if (params.contains(HasSelectedColsDefaultAsNull.SELECTED_COLS)) {
			return params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		}

		String[] featureCols = ArrayUtils.clone(tableSchema.getFieldNames());

		if (params.contains(HasWeightColDefaultAsNull.WEIGHT_COL)) {
			featureCols = ArrayUtils.removeElements(featureCols, params.get(HasWeightColDefaultAsNull.WEIGHT_COL));
		}

		if (params.contains(HasWeightCol.WEIGHT_COL)) {
			featureCols = ArrayUtils.removeElements(featureCols, params.get(HasWeightCol.WEIGHT_COL));
		}

		if (params.contains(HasGroupColDefaultAsNull.GROUP_COL)) {
			featureCols = ArrayUtils.removeElements(featureCols, params.get(HasGroupColDefaultAsNull.GROUP_COL));
		}

		if (params.contains(HasLabelCol.LABEL_COL)) {
			featureCols = ArrayUtils.removeElements(featureCols, params.get(HasLabelCol.LABEL_COL));
		}

		if (params.contains(HasLabelColDefaultAsNull.LABEL_COL)) {
			featureCols = ArrayUtils.removeElements(featureCols, params.get(HasLabelColDefaultAsNull.LABEL_COL));
		}

		return featureCols;
	}

	/**
	 * format the column names as header of markdown.
	 */
	public static String formatTitle(String[] colNames) {
		StringBuilder sbd = new StringBuilder();
		StringBuilder sbdSplitter = new StringBuilder();

		for (int i = 0; i < colNames.length; ++i) {
			if (i > 0) {
				sbd.append("|");
				sbdSplitter.append("|");
			}

			sbd.append(colNames[i]);
			if (null != colNames[i] && colNames[i].length() < 3) {
				for (int j = colNames[i].length(); j < 3; j++) {
					sbd.append(" ");
				}
			}

			int t = null == colNames[i] ? 4 : Math.max(colNames[i].length(), 3);
			for (int j = 0; j < t; j++) {
				sbdSplitter.append("-");
			}
		}

		return sbd + "\n" + sbdSplitter;
	}

	/**
	 * format the row as body of markdown.
	 */
	public static String formatRows(Row row) {
		StringBuilder sbd = new StringBuilder();
		StringBuilder[] subBuilders = new StringBuilder[DISPLAY_SIZE];
		int maxPos;
		int maxSubPos = 0;
		for (int i = 0; i < row.getArity(); ++i) {
			if (i > 0) {
				sbd.append("|");
			}
			Object obj = row.getField(i);
			if (obj instanceof Double || obj instanceof Float || obj instanceof BigDecimal) {
				sbd.append(String.format("%.4f", ((Number) obj).doubleValue()));
			} else if (obj instanceof DataTypeDisplayInterface) {
				if (obj instanceof DenseVector || obj instanceof SparseVector) {
					sbd.append(((DataTypeDisplayInterface) obj).toShortDisplayData());
					continue;
				}
				maxPos = sbd.length();
				sbd.append(((DataTypeDisplayInterface) obj).toDisplaySummary());
				String[] objStrArr = ((DataTypeDisplayInterface) obj).toShortDisplayData().split("\n");
				if (objStrArr.length == 1 && objStrArr[0].isEmpty()) {
					continue;
				}
				int minSize = Math.min(DISPLAY_SIZE, objStrArr.length);
				for (int k = 0; k < minSize; ++k) {
					if (subBuilders[k] == null) {
						subBuilders[k] = new StringBuilder();
					}
					if (subBuilders[k].length() < maxPos) {
						subBuilders[k].append(StringUtils.repeat(" ", maxPos - subBuilders[k].length() - 1));
					}
					if (k == DISPLAY_SIZE - 1) {
						if (i == 0) {
							subBuilders[k].append(" ... ... ");
						} else {
							if ('|' == subBuilders[k].charAt(subBuilders[k].length() - 1)) {
								subBuilders[k].append(" ... ... ");
							} else {
								subBuilders[k].append("|").append(" ... ... ");
							}
						}
					} else {
						if (i == 0) {
							subBuilders[k].append(objStrArr[k]);
						} else {
							if ('|' == subBuilders[k].charAt(subBuilders[k].length() - 1)) {
								subBuilders[k].append(objStrArr[k]);
							} else {
								subBuilders[k].append("|").append(objStrArr[k]);
							}
						}
					}
					maxSubPos = Math.max(maxSubPos, subBuilders[k].length());
				}
				int localMaxSize = Math.max(maxSubPos, sbd.length());
				for (int k = 0; k < minSize; ++k) {
					if (subBuilders[k].length() < localMaxSize) {
						subBuilders[k].append(StringUtils.repeat(" ", localMaxSize - subBuilders[k].length()));
					}
					if (i != row.getArity() - 1) {
						subBuilders[k].append("|");
					}
				}
				if (sbd.length() < maxSubPos) {
					for (int k = sbd.length(); k < maxSubPos; ++k) {
						sbd.append(" ");
					}
				}
			} else if (obj instanceof byte[]) {
				int byteSize = ((byte[]) obj).length;
				sbd.append("byte[").append(byteSize).append("] ");
				byte[] byteArray = byteSize > DISPLAY_SIZE ? Arrays.copyOfRange((byte[]) obj, 0, DISPLAY_SIZE)
					: (byte[]) obj;
				for (byte b : byteArray) {
					sbd.append(HEX.charAt((b >> 4) & 0x0f));
					sbd.append(HEX.charAt(b & 0x0f));
				}
				sbd.append((byteSize > DISPLAY_SIZE ? "..." : ""));
			} else if (obj instanceof Timestamp) {
				sbd.append((Timestamp) obj);
				//sbd.append(((Timestamp) obj).toLocalDateTime().atZone(ZoneId.systemDefault()));
				//sbd.append(Timestamp.valueOf(LocalDateTime.ofInstant(((Timestamp) obj).toInstant(), ZoneOffset.UTC)));
			} else {
				sbd.append(obj);
			}
		}
		if (subBuilders[0] != null) {
			for (int k = 0; k < DISPLAY_SIZE; ++k) {
				if (subBuilders[k] != null) {
					sbd.append("\n");
					sbd.append(subBuilders[k]);
				}
			}
		}
		return sbd.toString();
	}

	/**
	 * format the column names and rows in table as markdown.
	 */
	public static String format(String[] colNames, List <Row> data) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(formatTitle(colNames));

		for (Row row : data) {
			sbd.append("\n").append(formatRows(row));
		}

		return sbd.toString();
	}

	/**
	 * Convert column name array to SQL clause.
	 *
	 * <p>For example, columns "{a, b, c}" will be converted into a SQL-compatible select string section: "`a`, `b`,
	 * `c`".
	 *
	 * @param colNames columns to convert
	 * @return converted SQL clause.
	 */
	public static String columnsToSqlClause(String[] colNames) {
		return Joiner.on("`,`").appendTo(new StringBuilder("`"), colNames).append("`").toString();
	}

	//public static Table[] splitTable(Table table) {
	//	TableSchema schema = table.getSchema();
	//	final String[] colNames = schema.getFieldNames();
	//	String idCol = colNames[0];
	//	if (!idCol.equalsIgnoreCase("table_id")) {
	//		throw new AkIllegalArgumentException("The table can't be splited.");
	//	}
	//
	//	String lastCol = colNames[colNames.length - 1];
	//	int maxTableId = Integer.parseInt(lastCol.substring(1, lastCol.indexOf('_')));
	//	int numTables = maxTableId + 1;
	//
	//	int[] numColsOfEachTable = new int[numTables];
	//	for (int i = 1; i < colNames.length; i++) {
	//		int tableId = Integer.parseInt(colNames[i].substring(1, lastCol.indexOf('_')));
	//		numColsOfEachTable[tableId]++;
	//	}
	//
	//	Table[] splited = new Table[numTables];
	//	int startCol = 1;
	//	for (int i = 0; i < numTables; i++) {
	//		if (numColsOfEachTable[i] == 0) {
	//			continue;
	//		}
	//		String[] selectedCols = Arrays.copyOfRange(colNames, startCol, startCol + numColsOfEachTable[i]);
	//		BatchOperator <?> sub = BatchOperator.fromTable(table)
	//			.where(String.format("%s=%d", "table_id", i))
	//			.select(selectedCols);
	//
	//		// recover the col names
	//		String prefix = String.format("t%d_", i);
	//		StringBuilder sbd = new StringBuilder();
	//		for (int j = 0; j < selectedCols.length; j++) {
	//			if (j > 0) {
	//				sbd.append(",");
	//			}
	//			sbd.append(selectedCols[j].substring(prefix.length()));
	//		}
	//		sub = sub.as(sbd.toString());
	//		splited[i] = sub.getOutputTable();
	//		startCol += numColsOfEachTable[i];
	//	}
	//	return splited;
	//}

	public static Row getRow(Row row, int... keepIdxs) {
		Row res = null;
		if (null != keepIdxs) {
			res = new Row(keepIdxs.length);
			for (int i = 0; i < keepIdxs.length; i++) {
				res.setField(i, row.getField(keepIdxs[i]));
			}
		}
		return res;
	}

	/**
	 * Split a string by commas that are not inside parentheses or brackets.
	 *
	 * @param s string
	 * @return split strings.
	 */
	private static String[] robustSpiltByComma(String s) {
		List <String> splits = new ArrayList <>();
		char[] chars = s.toCharArray();
		int start = 0;
		int parenthesesLevel = 0;
		int angleBracketsLevel = 0;
		for (int i = 0; i < chars.length; i += 1) {
			char ch = chars[i];
			if (ch == '(') {
				parenthesesLevel += 1;
			} else if (ch == ')') {
				parenthesesLevel -= 1;
			} else if (ch == '<') {
				angleBracketsLevel += 1;
			} else if (ch == '>') {
				angleBracketsLevel -= 1;
			}
			if (ch == ',' && (parenthesesLevel == 0) && (angleBracketsLevel == 0)) {
				splits.add(new String(chars, start, i - start));
				start = i += 1;
			}
		}
		if (start < s.length()) {
			splits.add(new String(chars, start, s.length() - start));
		}
		return splits.toArray(new String[0]);
	}

	/**
	 * Extract the TableSchema from a string. The format of the string is comma separated colName-colType pairs,
	 * such as
	 * "f0 int,f1 bigint,f2 string".
	 *
	 * @param schemaStr The formatted schema string.
	 * @return TableSchema.
	 */
	public static TableSchema schemaStr2Schema(String schemaStr) {
		String[] fields = robustSpiltByComma(schemaStr);
		String[] colNames = new String[fields.length];
		TypeInformation <?>[] colTypes = new TypeInformation[fields.length];
		for (int i = 0; i < colNames.length; i++) {
			String[] kv = fields[i].trim().split("\\s+", 2);
			colNames[i] = kv[0];
			if (STRING_TYPE_SET.contains(kv[1])) {
				if (kv[1].startsWith("VEC_TYPES_")) {
					kv[1] = kv[1].substring("VEC_TYPES_".length());
				} else if (kv[1].startsWith("TENSOR_TYPES_")) {
					kv[1] = kv[1].substring("TENSOR_TYPES_".length());
				}
			}
			if (STRING_TYPE_MAP.containsKey(kv[1].toUpperCase())) {
				colTypes[i] = STRING_TYPE_MAP.get(kv[1].toUpperCase());
			} else {
				if (kv[1].contains("<") && kv[1].contains(">")) {
					colTypes[i] = FlinkTypeConverter.getFlinkType(kv[1]);
				} else {
					colTypes[i] = FlinkTypeConverter.getFlinkType(kv[1].toUpperCase());
				}
			}
		}
		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Transform the TableSchema to a string. The format of the string is comma separated colName-colType pairs,
	 * such as
	 * "f0 int,f1 bigint,f2 string".
	 *
	 * @param schema the TableSchema to transform.
	 * @return a string.
	 */
	public static String schema2SchemaStr(TableSchema schema) {
		String[] colNames = schema.getFieldNames();
		TypeInformation <?>[] colTypes = schema.getFieldTypes();

		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			String typeName;
			if (STRING_TYPE_MAP.containsValue(colTypes[i])) {
				typeName = STRING_TYPE_MAP.inverse().get(colTypes[i]);
			} else {
				typeName = FlinkTypeConverter.getTypeString(colTypes[i]);
			}
			sbd.append(colNames[i]).append(" ").append(typeName);
		}
		return sbd.toString();
	}

	/**
	 * Get column names from a schema string.
	 *
	 * @param schemaStr The formatted schema string.
	 * @return An array of column names.
	 */
	public static String[] getColNames(String schemaStr) {
		return schemaStr2Schema(schemaStr).getFieldNames();
	}

	/**
	 * Get column types from a schema string.
	 *
	 * @param schemaStr The formatted schema string.
	 * @return An array of column types.
	 */
	public static TypeInformation <?>[] getColTypes(String schemaStr) {
		return schemaStr2Schema(schemaStr).getFieldTypes();
	}
}
