package com.alibaba.alink.operator.batch.dl.ctr;

import com.google.gson.Gson;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat.ParseException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

public class EasyRecProtoUtils {

	private static final Gson gson = new Gson();

	static boolean isJsonFormat(String s) {
		try {
			gson.fromJson(s, Object.class);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static void mergeProtoStr(String s, Message.Builder builder) {
		if (isJsonFormat(s)) {
			try {
				JsonFormat.parser().merge(s, builder);
			} catch (InvalidProtocolBufferException e) {
				throw new RuntimeException(e);
			}
		} else {
			try {
				TextFormat.merge(s, builder);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static String messageToStr(MessageOrBuilder messageOrBuilder) {
		return TextFormat.printToString(messageOrBuilder);
	}
}
