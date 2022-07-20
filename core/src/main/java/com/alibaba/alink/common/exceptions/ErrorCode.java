package com.alibaba.alink.common.exceptions;

enum ErrorCode {
	// Codes of user type ranges from 0x0001 to 0x0fff
	ILLEGAL_OPERATOR_PARAMETER(Type.USER, Level.ERROR, 0x1L, "Illegal operator parameter"),
	COLUMN_NOT_FOUND(Type.USER, Level.ERROR, 0x2L, "Column not found"),
	ILLEGAL_DATA(Type.USER, Level.ERROR, 0x3L, "Illegal data"),
	PARSE_ERROR(Type.USER, Level.ERROR, 0x4L, "Parse error"),
	ILLEGAL_OPERATION(Type.USER, Level.ERROR, 0x5L, "Illegal operation"),
	UNSUPPORTED_OPERATION(Type.USER, Level.ERROR, 0x6L, "Unsupported operation"),
	ILLEGAL_MODEL(Type.USER, Level.ERROR, 0x7L, "Illegal model"),

	// Codes of user type ranges from 0x1001 to 0x1fff
	UNCLASSIFIED_ERROR(Type.PLATFORM, Level.ERROR, 0x1001L, "Unclassified error"),
	PLUGIN_ERROR(Type.PLATFORM, Level.ERROR, 0x1002L, "Plugin error"),
	FLINK_EXECUTION_ERROR(Type.PLATFORM, Level.ERROR, 0x1003L, "Flink execution error"),
	UNIMPLEMENTED_OPERATION(Type.PLATFORM, Level.ERROR, 0x1004L, "Unimplemented operation"),
	ILLEGAL_ARGUMENT(Type.PLATFORM, Level.ERROR, 0x1005L, "Illegal argument"),
	ILLEGAL_STATE(Type.PLATFORM, Level.ERROR, 0x1006L, "Illegal state"),
	NULL_POINTER(Type.PLATFORM, Level.ERROR, 0x1007L, "Null pointer"),
	;

	private final Type type;
	private final Level level;
	private final long code;
	private final String description;
	private final long totalErrorCode;

	ErrorCode(Type type, Level level, long code, String description) {
		this.type = type;
		this.level = level;
		this.code = code;
		this.description = description;
		totalErrorCode = calcTotalErrorCode();
	}

	long calcTotalErrorCode() {
		final long PRODUCT_CODE = 3;
		final long SUB_PRODUCT_CODE = 8;
		final long base = (type.id << 62L) | (level.id << 59L) | (PRODUCT_CODE << 53L) | (SUB_PRODUCT_CODE << 40L);
		return base + code;
	}

	@Override
	public String toString() {
		return String.format("%s: 0x%016x-%s", level, totalErrorCode, description);
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
