package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.pyrunner.fn.JavaObjectWrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

public class MTableWrapper implements JavaObjectWrapper <MTable> {

	private MTable mTable;
	private String[] colNames;
	private String schemaStr;
	private String content;

	public static MTableWrapper fromJava(MTable mTable) {
		MTableWrapper wrapper = new MTableWrapper();
		wrapper.mTable = mTable;
		wrapper.colNames = mTable.getColNames();
		wrapper.schemaStr = mTable.getSchemaStr();
		try (StringWriter stringWriter = new StringWriter();
			 BufferedWriter writer = new BufferedWriter(stringWriter)) {
			mTable.writeCsvToFile(writer);
			writer.flush();
			wrapper.content = stringWriter.toString();
		} catch (IOException e) {
			throw new RuntimeException("Failed to write MTable to StringWriter.", e);
		}
		return wrapper;
	}

	public static MTableWrapper fromPy(String content, String schemaStr) {
		MTableWrapper wrapper = new MTableWrapper();
		wrapper.content = content;
		wrapper.schemaStr = schemaStr;
		try (StringReader stringReader = new StringReader(content);
			 BufferedReader reader = new BufferedReader(stringReader)) {
			wrapper.mTable = MTable.readCsvFromFile(reader, schemaStr);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		return wrapper;
	}

	@Override
	public MTable getJavaObject() {
		return mTable;
	}

	public String[] getColNames() {
		return colNames;
	}

	public String getContent() {
		return content;
	}

	public String getSchemaStr() {
		return schemaStr;
	}
}
