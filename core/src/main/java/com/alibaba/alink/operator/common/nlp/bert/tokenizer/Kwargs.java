package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.common.exceptions.AkPreconditions;

import java.util.HashMap;

/**
 * A Java implementation of Python kwargs.
 */
public class Kwargs extends HashMap <String, Object> {

	private Kwargs() {
	}

	public static Kwargs empty() {
		return new Kwargs();
	}

	/**
	 * Initialize by providing key-value pairs.
	 *
	 * @param args multiple key-value pairs, where keys must be {@link String}
	 * @return a Kwargs instance
	 */
	public static Kwargs of(Object... args) {
		return new Kwargs().putIfAbsent(args);
	}

	public Kwargs clone() {
		super.clone();
		Kwargs kwargs = Kwargs.of();
		kwargs.putAll(this);
		return kwargs;
	}

	/**
	 * Set values for absent keys.
	 *
	 * @param args multiple key-value pairs, where keys must be {@link String}
	 * @return
	 */
	public Kwargs putIfAbsent(Object... args) {
		AkPreconditions.checkArgument(args.length % 2 == 0, "#args must be a multiple of 2.");
		for (int i = 0; i < args.length; i += 2) {
			String key = (String) args[i];
			Object value = args[i + 1];
			putIfAbsent(key, value);
		}
		return this;
	}

	public Kwargs put(Object... args) {
		AkPreconditions.checkArgument(args.length % 2 == 0, "#args must be a multiple of 2.");
		for (int i = 0; i < args.length; i += 2) {
			String key = (String) args[i];
			Object value = args[i + 1];
			put(key, value);
		}
		return this;
	}

	public <T> T getWithType(String key) {
		//noinspection unchecked
		return (T) get(key);
	}

	public <T> T removeWithType(String key) {
		//noinspection unchecked
		return (T) remove(key);
	}
}
