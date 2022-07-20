// ----------------------------------------------------------------------------
//  This class is adapted from "org.apache.flink.util.Preconditions", which is part of the "flink-core" library.
//
//  Because of incompatibility of exception classes, this class was added to the Alink code base.
// ----------------------------------------------------------------------------
package com.alibaba.alink.common.exceptions;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * A collection of static utility methods to validate input.
 *
 * <p>This class is modelled after Flink's Preconditions class, and partly takes code
 * from that class. We add this code to the Alink code base in order to throw different exception classes.
 */
@Internal
public final class AkPreconditions {

	// ------------------------------------------------------------------------
	//  Null checks
	// ------------------------------------------------------------------------

	/**
	 * Ensures that the given object reference is not null. Upon violation, a {@code AkNullPointerException} with no
	 * message is thrown.
	 *
	 * @param reference The object reference
	 * @return The object reference itself (generically typed).
	 * @throws AkNullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference) {
		if (reference == null) {
			throw new AkNullPointerException(null);
		}
		return reference;
	}

	/**
	 * Ensures that the given object reference is not null. Upon violation, a {@code AkNullPointerException} with the
	 * given message is thrown.
	 *
	 * @param reference    The object reference
	 * @param errorMessage The message for the {@code AkNullPointerException} that is thrown if the check fails.
	 * @return The object reference itself (generically typed).
	 * @throws AkNullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference, @Nullable String errorMessage) {
		if (reference == null) {
			throw new AkNullPointerException(String.valueOf(errorMessage));
		}
		return reference;
	}

	/**
	 * Ensures that the given object reference is not null. Upon violation, a {@code AkNullPointerException} with the
	 * given message is thrown.
	 *
	 * <p>The error message is constructed from a template and an arguments array, after
	 * a similar fashion as {@link String#format(String, Object...)}, but supporting only {@code %s} as a placeholder.
	 *
	 * @param reference            The object reference
	 * @param errorMessageTemplate The message template for the {@code AkNullPointerException} that is thrown if the
	 *                             check fails. The template substitutes its {@code %s} placeholders with the error
	 *                             message arguments.
	 * @param errorMessageArgs     The arguments for the error message, to be inserted into the message template for the
	 *                             {@code %s} placeholders.
	 * @return The object reference itself (generically typed).
	 * @throws AkNullPointerException Thrown, if the passed reference was null.
	 */
	public static <T> T checkNotNull(T reference,
									 @Nullable String errorMessageTemplate,
									 @Nullable Object... errorMessageArgs) {

		if (reference == null) {
			throw new AkNullPointerException(format(errorMessageTemplate, errorMessageArgs));
		}
		return reference;
	}

	public static <T> T checkNotNull(T reference, ExceptionWithErrorCode exception) {
		if (reference == null) {
			throw exception;
		}
		return reference;
	}

	// ------------------------------------------------------------------------
	//  Boolean Condition Checking (Argument)
	// ------------------------------------------------------------------------

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalArgumentException} if the condition is not met
	 * (evaluates to {@code false}).
	 *
	 * @param condition The condition to check
	 * @throws AkIllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition) {
		if (!condition) {
			throw new AkIllegalArgumentException(null);
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalArgumentException} if the condition is not met
	 * (evaluates to {@code false}). The exception will have the given error message.
	 *
	 * @param condition    The condition to check
	 * @param errorMessage The message for the {@code AkIllegalArgumentException} that is thrown if the check fails.
	 * @throws AkIllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition, @Nullable Object errorMessage) {
		if (!condition) {
			throw new AkIllegalArgumentException(String.valueOf(errorMessage));
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalArgumentException} if the condition is not met
	 * (evaluates to {@code false}).
	 *
	 * @param condition            The condition to check
	 * @param errorMessageTemplate The message template for the {@code AkIllegalArgumentException} that is thrown if the
	 *                             check fails. The template substitutes its {@code %s} placeholders with the error
	 *                             message arguments.
	 * @param errorMessageArgs     The arguments for the error message, to be inserted into the message template for the
	 *                             {@code %s} placeholders.
	 * @throws AkIllegalArgumentException Thrown, if the condition is violated.
	 */
	public static void checkArgument(boolean condition,
									 @Nullable String errorMessageTemplate,
									 @Nullable Object... errorMessageArgs) {

		if (!condition) {
			throw new AkIllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
		}
	}

	public static void checkArgument(boolean condition, ExceptionWithErrorCode exception) {
		if (!condition) {
			throw exception;
		}
	}

	// ------------------------------------------------------------------------
	//  Boolean Condition Checking (State)
	// ------------------------------------------------------------------------

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalStateException} if the condition is not met
	 * (evaluates to {@code false}).
	 *
	 * @param condition The condition to check
	 * @throws AkIllegalStateException Thrown, if the condition is violated.
	 */
	public static void checkState(boolean condition) {
		if (!condition) {
			throw new AkIllegalStateException(null);
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalStateException} if the condition is not met
	 * (evaluates to {@code false}). The exception will have the given error message.
	 *
	 * @param condition    The condition to check
	 * @param errorMessage The message for the {@code AkIllegalStateException} that is thrown if the check fails.
	 * @throws AkIllegalStateException Thrown, if the condition is violated.
	 */
	public static void checkState(boolean condition, @Nullable Object errorMessage) {
		if (!condition) {
			throw new AkIllegalStateException(String.valueOf(errorMessage));
		}
	}

	/**
	 * Checks the given boolean condition, and throws an {@code AkIllegalStateException} if the condition is not met
	 * (evaluates to {@code false}).
	 *
	 * @param condition            The condition to check
	 * @param errorMessageTemplate The message template for the {@code AkIllegalStateException} that is thrown if the
	 *                             check fails. The template substitutes its {@code %s} placeholders with the error
	 *                             message arguments.
	 * @param errorMessageArgs     The arguments for the error message, to be inserted into the message template for the
	 *                             {@code %s} placeholders.
	 * @throws AkIllegalStateException Thrown, if the condition is violated.
	 */
	public static void checkState(boolean condition,
								  @Nullable String errorMessageTemplate,
								  @Nullable Object... errorMessageArgs) {

		if (!condition) {
			throw new AkIllegalStateException(format(errorMessageTemplate, errorMessageArgs));
		}
	}

	public static void checkState(boolean condition, ExceptionWithErrorCode exception) {
		if (!condition) {
			throw exception;
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * A simplified formatting method. Similar to {@link String#format(String, Object...)}, but with lower overhead
	 * (only String parameters, no locale, no format validation).
	 *
	 * <p>This method is taken quasi verbatim from the Guava Preconditions class.
	 */
	private static String format(@Nullable String template, @Nullable Object... args) {
		final int numArgs = args == null ? 0 : args.length;
		template = String.valueOf(template); // null -> "null"

		// start substituting the arguments into the '%s' placeholders
		StringBuilder builder = new StringBuilder(template.length() + 16 * numArgs);
		int templateStart = 0;
		int i = 0;
		while (i < numArgs) {
			int placeholderStart = template.indexOf("%s", templateStart);
			if (placeholderStart == -1) {
				break;
			}
			builder.append(template, templateStart, placeholderStart);
			builder.append(args[i++]);
			templateStart = placeholderStart + 2;
		}
		builder.append(template.substring(templateStart));

		// if we run out of placeholders, append the extra args in square braces
		if (i < numArgs) {
			builder.append(" [");
			builder.append(args[i++]);
			while (i < numArgs) {
				builder.append(", ");
				builder.append(args[i++]);
			}
			builder.append(']');
		}

		return builder.toString();
	}

	// ------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation.
	 */
	private AkPreconditions() {}
}
