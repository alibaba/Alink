package com.alibaba.alink.common.io.filesystem.copy.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;

public class BoundedRowCsvInputFormat extends RowCsvInputFormat {
	private static final long serialVersionUID = 4401493087002246088L;
	int capacity = 0;

	public BoundedRowCsvInputFormat(Path filePath, TypeInformation[] fieldTypeInfos, String lineDelimiter,
									String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull,
									BaseFileSystem <?> fs, int capacity) {
		super(filePath, fieldTypeInfos, lineDelimiter, fieldDelimiter, selectedFields, emptyColumnAsNull, fs);
		this.capacity = capacity;
	}

	@Override
	public boolean reachedEnd() {
		capacity--;
		if (capacity < 0) {
			this.setReachedEnd();
		}
		return super.reachedEnd();
	}
}
