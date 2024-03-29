package org.apache.flink.ml.api.misc.param;

import org.apache.flink.types.Row;

import com.alibaba.alink.params.ParamUtil;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Container class for parameters.
 */
public class Params implements Serializable {
	private static final long serialVersionUID = 7512382732224963858L;

	private static Gson pGson =
		new GsonBuilder()
			.serializeNulls()
			.disableHtmlEscaping()
			.serializeSpecialFloatingPointValues()
			.create();

	private HashMap <String, String> params;

	public Params() {
		this.params = new HashMap <>();
	}

	@SuppressWarnings("unchecked")
	public static Params fromJson(String jsonString) {
		Params ret = new Params();
		ret.params = pGson.fromJson(jsonString, ret.params.getClass());
		return ret;
	}

	public int size() {
		return params.size();
	}

	public void clear() {
		params.clear();
	}

	public boolean isEmpty() {
		return params.isEmpty();
	}

	private Map<String, Object[]> rowToJsonMap(Row row) {
		if (null == row) {
			return null;
		}
		if (row.getArity() > 0) {
			Object[] objects = new Object[row.getArity()];
			for (int i = 0; i < objects.length; i++) {
				objects[i] = row.getField(i);
			}

			Map<String, Object[]> jsonMap = new HashMap <>();
			jsonMap.put("fields", objects);

			return jsonMap;
		}
		return new HashMap <>();
	}

	private Row jsonMapToRow(Map<String, Object[]> jsonMap) {
		if (null == jsonMap) {
			return null;
		}
		Object[] objects = jsonMap.get("fields");
		Row row = new Row(objects.length);
		for (int i = 0; i < objects.length; i++) {
			row.setField(i, objects[i]);
		}
		return row;
	}

	@Override
	public Params clone() {
		Params cloneParams = new Params();
		cloneParams.params = new HashMap <>(this.params);
		return cloneParams;
	}

	public void remove(String paramName) {
		this.params.remove(paramName);
	}

	public Params merge(Params otherParams) {
		if (null != otherParams) {
			for (Map.Entry <String, String> entry : otherParams.params.entrySet()) {
				this.params.put(entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public Params setIgnoreNull(String paramName, Object paramValue) {
		if (null == paramValue) {
			return this;
		} else {
			return set(paramName, paramValue);
		}
	}

	public Params set(String paramName, Object paramValue) {
		if (null == paramValue) {
			this.params.put(paramName, null);
		} else if (paramValue.getClass().equals(Row[].class)) {
			Row[] rows = (Row[]) paramValue;
			ArrayList <Map<String, Object[]>> jsonMapList = new ArrayList <>();
			for (int i = 0; i < rows.length; i++) {
				jsonMapList.add(rowToJsonMap(rows[i]));
			}
			this.params.put(paramName, pGson.toJson(jsonMapList));
		} else if (paramValue.getClass().equals(Row.class)) {
			this.params.put(paramName, pGson.toJson(rowToJsonMap((Row) paramValue)));
		}
		else {
			this.params.put(paramName, pGson.toJson(paramValue));
		}
		return this;
	}

	public Params set(String paramName, Object paramValue, Class paramClass) {
		this.params.put(paramName, pGson.toJson(paramValue, paramClass));
		return this;
	}

	public <V> Params set(ParamInfo <V> paramInfo, V value) {
		ParamValidator validator = paramInfo.getValidator();
		if (validator != null) {
			validator.validateThrows(value);
		}
		return set(paramInfo.getName(), value);
	}

	private <V> Stream <String> getParamNameAndAlias(
		ParamInfo <V> paramInfo) {
		Stream <String> paramNames = Stream.of(paramInfo.getName());
		if (null != paramInfo.getAlias() && paramInfo.getAlias().length > 0) {
			paramNames = Stream.concat(paramNames, Arrays.stream(paramInfo.getAlias())).sequential();
		}
		return paramNames;
	}

	public <V> V getValue(ParamInfo <V> paramInfo) {
		Stream <V> paramValue = getParamNameAndAlias(paramInfo)
			.filter(this::contains)
			.map(x -> this.get(x, paramInfo.getValueClass()))
			.limit(1);
		if (paramInfo.isOptional()) {
			if (paramInfo.hasDefaultValue()) {
				return paramValue.reduce(paramInfo.getDefaultValue(), (a, b) -> b);
			} else {
				return paramValue.collect(Collectors.collectingAndThen(Collectors.toList(),
					a -> {
						if (a.isEmpty()) {
							throw new RuntimeException("Not have defaultValue for parameter: " + paramInfo.getName());
						}
						return a.get(0);
					}));
			}
		}
		return paramValue.collect(Collectors.collectingAndThen(Collectors.toList(),
			a -> {
				if (a.isEmpty()) {
					throw new RuntimeException("Not have parameter: " + paramInfo.getName());
				}
				return a.get(0);
			}));
	}

	public <V> V get(ParamInfo <V> paramInfo) {
		V v = getValue(paramInfo);
		ParamValidator validator = paramInfo.getValidator();
		if (validator != null) {
			validator.validateThrows(v);
		}
		return v;
	}

	public <V> boolean contains(ParamInfo <V> paramInfo) {
		return getParamNameAndAlias(paramInfo).anyMatch(this::contains);
	}

	public String toJson() {
		return pGson.toJson(this.params);
	}

	public boolean contains(String paramName) {
		return params.containsKey(paramName);
	}

	public boolean contains(String[] paramNames) {
		if (null == paramNames) {
			return true;
		}
		for (String paramName : paramNames) {
			if (!params.containsKey(paramName)) {
				return false;
			}
		}
		return true;
	}

	public Set <String> listParamNames() {
		return params.keySet();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> T get(String paramName, Class <T> classOfT) {
		if (classOfT.isEnum()) {
			String value = get(paramName, String.class);
			return (T) ParamUtil.searchEnum((Class) classOfT, value, paramName);
		} else if (classOfT.equals(Row[].class)) {
			List<Map<String, Object[]>> jsonMapList = get(paramName, new TypeToken<List <Map<String, Object[]>>>() {}.getType());
			if (null == jsonMapList) {
				return null;
			}

			Row[] rows = new Row[jsonMapList.size()];
			for (int i = 0; i < jsonMapList.size(); i++) {
				rows[i] = jsonMapToRow(jsonMapList.get(i));
			}
			return (T) rows;
		} else if (classOfT.equals(Row.class)) {
			return (T) jsonMapToRow(get(paramName, new TypeToken<Map<String, Object[]>>() {}.getType()));
		} else {
			return get(paramName, (Type) classOfT);
		}
	}

	public <T> T get(String paramName, Type typeOfT) {
		if (!this.params.containsKey(paramName)) {
			throw new RuntimeException("Not have parameter : " + paramName);
		} else {
			String paramValue = this.params.get(paramName);
			try {
				return pGson.fromJson(paramValue, typeOfT);
			} catch (Exception ex) {
				throw new RuntimeException("Error in fromJson the paramValue : " + paramName + "\n" + ex.getMessage
					());
			}
		}
	}

	public <T> T getOrDefault(String paramName, Class <T> classOfT, Object defaultValue) {
		if (this.params.containsKey(paramName)) {
			return get(paramName, classOfT);
		} else {
			if (null == defaultValue) {
				return null;
			} else if (defaultValue.getClass().equals(classOfT)) {
				return (T) defaultValue;
			} else {
				throw new RuntimeException("Wrong class type of default value.");
			}
		}
	}

	//public Params getParams(String paramName) {
	//    return get(paramName, Params.class);
	//}

	public String getString(String paramName) {
		return get(paramName, String.class);
	}

	public String getStringOrDefault(String paramName, String defaultValue) {
		return getOrDefault(paramName, String.class, defaultValue);
	}

	@Override
	public String toString() {
		return "Params " + params;
	}

	public Boolean getBool(String paramName) {
		return get(paramName, Boolean.class);
	}

	public Boolean getBoolOrDefault(String paramName, Boolean defaultValue) {
		return getOrDefault(paramName, Boolean.class, defaultValue);
	}

	public Integer getInteger(String paramName) {
		return get(paramName, Integer.class);
	}

	public Integer getIntegerOrDefault(String paramName, Integer defaultValue) {
		return getOrDefault(paramName, Integer.class, defaultValue);
	}

	public Long getLong(String paramName) {
		return get(paramName, Long.class);
	}

	public Long getLongOrDefault(String paramName, Long defaultValue) {
		return getOrDefault(paramName, Long.class, defaultValue);
	}

	public Double getDouble(String paramName) {
		return get(paramName, Double.class);
	}

	public Double getDoubleOrDefault(String paramName, Double defaultValue) {
		return getOrDefault(paramName, Double.class, defaultValue);
	}

	//public Double[] getDoubleArray(String paramName) {
	//    return get(paramName, Double[].class);
	//}

	public Double[] getDoubleArrayOrDefault(String paramName, Double[] defaultValue) {
		return getOrDefault(paramName, Double[].class, defaultValue);
	}

	public String[] getStringArray(String paramName) {
		return get(paramName, String[].class);
	}

	public String[] getStringArrayOrDefault(String paramName, String[] defaultValue) {
		return getOrDefault(paramName, String[].class, defaultValue);
	}

	public Integer[] getIntegerArray(String paramName) {
		return get(paramName, Integer[].class);
	}

	//public Integer[] getIntegerArrayOrDefault(String paramName, Integer[] defaultValue) {
	//    return getOrDefault(paramName, Integer[].class, defaultValue);
	//}

	public Long[] getLongArray(String paramName) {
		return get(paramName, Long[].class);
	}

	//public Long[] getLongArrayOrDefault(String paramName, Long[] defaultValue) {
	//    return getOrDefault(paramName, Long[].class, defaultValue);
	//}

	public <V> void remove(ParamInfo <V> info) {
		remove(info.getName());
	}

	public void loadJson(String json) {
		throw new RuntimeException("Unsupported now.");
	}
}
