package com.alibaba.alink.python.utils;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Util for the parameters.
 */
public class ParamUtil {

	private static Stream<Field> getAllFields(Class cls) {
		final List<Class> ret = new ArrayList<>();
		ret.add(cls);
		final Set<Class> hash = new HashSet<>(Collections.singletonList(cls));
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
	 * given the class of an operator, list all parameters
	 *
	 * @param cls the class of operator
	 * @return
	 */
	public static List <ParamInfo> getParametersByOperator(Class cls) {
		Stream <ParamInfo> s = getAllFields(cls)
			.filter(x -> Modifier.isStatic(x.getModifiers()) && Modifier.isFinal(x.getModifiers()))
			.filter(x -> ParamInfo.class.isAssignableFrom(x.getType()))
			.map(x -> {
				try {
					return (ParamInfo) x.get(null);
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
						return ((ParamInfo) x.invoke(obj));
					} catch (IllegalAccessException | InvocationTargetException e) {
						return null;
					}
				})
				.filter(Objects::nonNull))
				.distinct();
		} catch (Exception e) {
		}

		return s.sorted(Comparator
			.<ParamInfo, Boolean>comparing(ParamInfo::isOptional)
			.thenComparing(ParamInfo::getName))
			.collect(Collectors.toList());
	}

	public static List <ImmutableTriple <Class, Field, ParamInfo>> getParameterClassByOperator(Class cls) {
		List <ImmutableTriple <Class, Field, ParamInfo>> params = new ArrayList <>();

		final Set <Class> IgnoredClasses = Stream.of(WithParams.class)
			.collect(Collectors.toSet());

		Deque <Class> Q = new LinkedList <>();
		Q.push(cls);

		while (!Q.isEmpty()) {
			Class clazz = Q.pollFirst();

			if (IgnoredClasses.contains(clazz)) {
				continue;
			}

			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if (field.getType() == ParamInfo.class) {
					try {
						ParamInfo paramInfo = (ParamInfo) field.get(null);
						if (paramInfo.getName().equals("MLEnvironmentId")) {
							continue;
						}
						params.add(new ImmutableTriple<>(clazz, field, paramInfo));
					} catch (Exception e) {
					}
				}
			}

			Class[] classes = clazz.getInterfaces();
			Q.addAll(Arrays.asList(classes));
			if (null != clazz.getSuperclass()) {
				Q.add(clazz.getSuperclass());
			}
		}
		return params;
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
	 * given one operator, print the help information
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
}
