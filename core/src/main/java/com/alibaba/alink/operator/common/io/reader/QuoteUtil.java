package com.alibaba.alink.operator.common.io.reader;

import java.io.IOException;

public class QuoteUtil {
	private static final int BUFFER_SIZE = 1024 * 1024;

	public static long analyzeSplit(FileSplitReader reader,byte quoteCharacter) throws IOException {
		byte[] buf = new byte[BUFFER_SIZE];
		long splitLength = reader.getSplitLength();
		long byteRead = 0;
		long quoteNum = 0;
		int read;

		while (byteRead < splitLength) {
			read = reader.read(buf, 0, (int) Long.min(BUFFER_SIZE, splitLength - byteRead));
			if (read > 0) {
				byteRead += read;
			}else{
				break;
			}

			for (int i = 0; i < read; i++) {
				if (buf[i] == quoteCharacter) {
					quoteNum++;
				}
			}
		}
		return quoteNum;
	}
	
}
