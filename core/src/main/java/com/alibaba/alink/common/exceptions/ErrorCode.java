package com.alibaba.alink.common.exceptions;

enum ErrorCode {
	UNCLASSIFIED_ERROR(Type.PLATFORM, Level.ERROR, 1L, "Unclassified error"),
	PLUGIN_ERROR(Type.PLATFORM, Level.ERROR, 2L, "Plugin error"),
	FLINK_EXECUTION_ERROR(Type.PLATFORM, Level.ERROR, 3L, "Flink execution error"),
	UNIMPLEMENTED_OPERATION(Type.PLATFORM, Level.ERROR, 4L, "Unimplemented operation"),

	ILLEGAL_ARGUMENT(Type.USER, Level.ERROR, 32L, "Illegal argument"),
	ILLEGAL_OPERATOR_PARAMETER(Type.USER, Level.ERROR, 33L, "Illegal operator parameter"),
	COLUMN_NOT_FOUND(Type.USER, Level.ERROR, 34L, "Column not found"),
	ILLEGAL_DATA(Type.USER, Level.ERROR, 35L, "Illegal data"),
	PARSE_ERROR(Type.USER, Level.ERROR, 36L, "Parse error"),
	ILLEGAL_OPERATION(Type.USER, Level.ERROR, 37L, "Illegal operation"),
	UNSUPPORTED_OPERATION(Type.USER, Level.ERROR, 38L, "Unsupported operation"),
	;

	private final Type type;
	private final Level level;
	private final long code;
	private final String description;
	private final long allErrorCode;

	ErrorCode(Type type, Level level, long code, String description) {
		this.type = type;
		this.level = level;
		this.code = code;
		this.description = description;
		allErrorCode = calcAllErrorCode();
	}

	private long calcAllErrorCode() {
		final long PAI_PRODUCT_CODE = 3;
		final long ALINK_SUB_PRODUCT_CODE = 8;
		return (type.id << 62L) | (level.id << 59L)
			| (PAI_PRODUCT_CODE << 53L) | (ALINK_SUB_PRODUCT_CODE << 40L)
			| code;
	}

	@Override
	public String toString() {
		return String.format("%s: %d-%s", level, allErrorCode, description);
	}

	enum Type {
		PLATFORM(0),
		USER(1),
		OTHER(2);

		private final long id;

		Type(long id) {
			this.id = id;
		}
	}

	// Only use `ERROR` level.
	enum Level {
		ERROR(1);

		private final long id;

		Level(long id) {
			this.id = id;
		}
	}
}
