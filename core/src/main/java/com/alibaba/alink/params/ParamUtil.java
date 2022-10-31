package com.alibaba.alink.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Util for the parameters.
 */
public class ParamUtil {

	private static Stream <Field> getAllFields(Class cls) {
		final List <Class> ret = new ArrayList <>();
		ret.add(cls);
		final Set <Class> hash = new HashSet <>(Collections.singletonList(cls));
		for (int i = 0; i < ret.size(); ++i) {
			Class cur = ret.get(i);
			Optional.ofNullable(cur.getSuperclass()).map(ret::add);
			Stream.of(cur.getInterfaces())
				.filter(x -> !hash.contains(x))
				.forEach(x -> {
					ret.add(x);
					hash.add(x);
				});
		}
		return ret.stream()
			.flatMap(x -> Stream.of(x.getDeclaredFields()));
	}

	/**
	 * Given the class of an operator, list all parameters
	 *
	 * @param cls the class of operator
	 * @return
	 */
	public static List <ParamInfo <?>> getParametersByOperator(Class <?> cls) {
		Stream <ParamInfo <?>> s = getAllFields(cls)
			.filter(x -> Modifier.isStatic(x.getModifiers()) && Modifier.isFinal(x.getModifiers()))
			.filter(x -> ParamInfo.class.isAssignableFrom(x.getType()))
			. <ParamInfo <?>>map(x -> {
				try {
					return (ParamInfo <?>) x.get(null);
				} catch (Exception ex) {
					return null;
				}
			})
			.filter(Objects::nonNull);

		try {
			final Object obj = cls.getConstructor(Params.class).newInstance(new Params());
			s = Stream.concat(s, Stream.of(cls.getMethods())
				.filter(x -> !Modifier.isStatic(x.getModifiers()) && x.getParameterCount() == 0)
				.filter(x -> ParamInfo.class.isAssignableFrom(x.getReturnType()))
				.map(x -> {
					try {
						return ((ParamInfo <?>) x.invoke(obj));
					} catch (IllegalAccessException | InvocationTargetException e) {
						return null;
					}
				})
				.filter(Objects::nonNull))
				.distinct();
		} catch (Exception e) {
		}

		return s.sorted((a, b) -> a.isOptional() ? (b.isOptional() ? 0 : 1) : (b.isOptional() ? -1 : 0))
			.collect(Collectors.toList());
	}

	private static void printOneRow(String[] cells, int[] maxLength) {
		System.out.print("|");
		for (int i = 0; i < cells.length; ++i) {
			System.out.print(" ");
			System.out.print(cells[i]);
			System.out.print(StringUtils.repeat(" ", maxLength[i] - cells[i].length()));
			System.out.print(" |");
		}
		System.out.println();
	}

	/**
	 * Given one operator, print the help information
	 *
	 * @param cls the class of operator
	 */
	public static void help(Class cls) {
		List <String[]> g = getParametersByOperator(cls).stream()
			.map(x -> new String[] {
				x.getName(),
				x.getDescription(),
				x.isOptional() ? "optional" : "required",
				x.getDefaultValue() == null ? "null" : JsonConverter.gson.toJson(x.getDefaultValue())
			})
			.collect(Collectors.toList());

		final String[] tableHeader = new String[] {"name", "description", "optional", "defaultValue"};

		final int[] maxLengthOfCells = IntStream.range(0, tableHeader.length)
			.map(idx -> Math.max(tableHeader[idx].length(),
				g.stream().mapToInt(x -> x[idx].length()).max().orElse(0)))
			.toArray();

		final int maxLength = IntStream.of(maxLengthOfCells).sum() + maxLengthOfCells.length * 3 + 1;

		System.out.println(StringUtils.repeat("-", maxLength));
		printOneRow(tableHeader, maxLengthOfCells);
		System.out.println(StringUtils.repeat("-", maxLength));

		g.forEach(x -> printOneRow(x, maxLengthOfCells));
		System.out.println(StringUtils.repeat("-", maxLength));
	}

	/**
	 * Convert string to enum, and throw exception.
	 *
	 * @param enumeration enum class
	 * @param search      search item
	 * @param paramName   param name
	 * @param <T>         class
	 * @return enum
	 */
	public static <T extends Enum <?>> T searchEnum(Class <T> enumeration, String search, String paramName) {
		return searchEnum(enumeration, search, paramName, null);
	}

	/**
	 * Convert string to enum, and throw exception.
	 *
	 * @param paramInfo paramInfo
	 * @param search    search item
	 * @param <T>       class
	 * @return enum
	 */
	public static <T extends Enum <?>> T searchEnum(ParamInfo <T> paramInfo, String search) {
		return searchEnum(paramInfo.getValueClass(), search, paramInfo.getName());
	}

	/**
	 * Convert string to enum, and throw exception.
	 *
	 * @param enumeration enum class
	 * @param search      search item
	 * @param paramName   param name
	 * @param opName      op name
	 * @param <T>         class
	 * @return enum
	 */
	public static <T extends Enum <?>> T searchEnum(Class <T> enumeration, String search, String paramName,
													String opName) {
		if (search == null) {
			return null;
		}
		T[] values = enumeration.getEnumConstants();
		for (T each : values) {
			if (each.name().compareToIgnoreCase(search) == 0) {
				return each;
			}
		}

		StringBuilder sbd = new StringBuilder();
		sbd.append(search)
			.append(" is not member of ")
			.append(paramName);
		if (opName != null && opName.isEmpty()) {
			sbd.append(" of ")
				.append(opName);
		}
		sbd.append(".")
			.append("It maybe ")
			.append(values[0].name());
		for (int i = 1; i < values.length; i++) {
			sbd.append(",")
				.append(values[i].name());
		}
		sbd.append(".");
		throw new AkIllegalOperatorParameterException(sbd.toString());
	}

}
