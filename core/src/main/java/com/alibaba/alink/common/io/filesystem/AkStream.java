package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.binary.BinaryRecordReader;
import com.alibaba.alink.common.io.filesystem.binary.BinaryRecordWriter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Single file only.
 */
public class AkStream {

	private FilePath filePath;

	private AkUtils.AkMeta akMeta;

	public AkStream(FilePath filePath, AkUtils.AkMeta akMeta) throws IOException {
		Preconditions.checkNotNull(filePath);
		this.filePath = filePath;
		if (akMeta == null) {
			this.akMeta = initialMeta(filePath);
		} else {
			this.akMeta = akMeta;
		}
	}

	public AkUtils.AkMeta getAkMeta() {
		return akMeta;
	}

	public FilePath getFilePath() {
		return filePath;
	}

	public AkReader getReader() throws IOException {
		return new AkReader();
	}

	public AkWriter getWriter() throws IOException {
		return new AkWriter();
	}

	public class AkReader implements Iterable<Row> {
		ZipInputStream inputStream;

		AkReader() throws IOException {
			inputStream = new ZipInputStream(
				new BufferedInputStream(
					AkStream.this.filePath.getFileSystem().open(AkStream.this.filePath.getPath())
				)
			);
		}

		AkReader(InputStream inputStream) {
			this.inputStream = new ZipInputStream(new BufferedInputStream(inputStream));
		}

		@Override
		public AkReadIterator iterator() {
			return new AkReadIterator();
		}

		public class AkReadIterator implements Iterator<Row> {
			BinaryRecordReader binaryRecordReader;

			AkReadIterator() {
				binaryRecordReader = new BinaryRecordReader(
					inputStream,
					CsvUtil.getColNames(AkStream.this.akMeta.schemaStr),
					CsvUtil.getColTypes(AkStream.this.akMeta.schemaStr)
				);
			}

			@Override
			public boolean hasNext() {
				try {
					while (!binaryRecordReader.hasNextRecord()) {
						ZipEntry entry;
						while ((entry = inputStream.getNextEntry()) != null) {
							if (!entry.isDirectory()
								&& !entry.getName().equalsIgnoreCase(AkUtils.META_FILE)) {
								break;
							}
						}
						if (entry != null) {
							binaryRecordReader.readAndCheckHeader();
						} else {
							return false;
						}
					}
					return true;
				} catch (IOException e) {
					throw new RuntimeException("Could not get the next read.", e);
				}
			}

			@Override
			public Row next() {
				try {
					return binaryRecordReader.getNextRecord();
				} catch (IOException e) {
					throw new RuntimeException("Could not get the next read.", e);
				}
			}
		}

		public void close() throws IOException {
			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
			}
		}
	}

	public class AkWriter {
		ZipOutputStream zipOutputStream;

		AkWriter() throws IOException {
			zipOutputStream =
				new ZipOutputStream(
					new BufferedOutputStream(
						AkStream.this.filePath.getFileSystem()
							.create(
								AkStream.this.filePath.getPath(),
								FileSystem.WriteMode.OVERWRITE
							)
					)
				);

			// should be initial the num files in meta.
			writeMeta2Stream(akMeta, zipOutputStream);
		}

		AkWriter(OutputStream outputStream) throws IOException {
			zipOutputStream = new ZipOutputStream(new BufferedOutputStream(outputStream));

			// should be initial the num files in meta.
			writeMeta2Stream(akMeta, zipOutputStream);
		}

		public AkCollector getCollector() {
			try {
				return new AkCollector();
			} catch (IOException e) {
				throw new RuntimeException("Could not get the collector.", e);
			}
		}

		public class AkCollector implements Collector<Row> {
			BinaryRecordWriter binaryRecordWriter;

			AkCollector() throws IOException {
				// Could set the new file name here.
				zipOutputStream.putNextEntry(new ZipEntry(AkUtils.DATA_FILE));
				binaryRecordWriter = new BinaryRecordWriter(
					AkWriter.this.zipOutputStream,
					CsvUtil.getColNames(AkStream.this.akMeta.schemaStr),
					CsvUtil.getColTypes(AkStream.this.akMeta.schemaStr)
				);
				binaryRecordWriter.writeHeader();
			}

			@Override
			public void collect(Row record) {
				try {
					binaryRecordWriter.writeRecord(record);
				} catch (IOException e) {
					throw new RuntimeException("Write the record fail.", e);
				}
			}

			@Override
			public void close() {
				try {
					AkWriter.this.close();
				} catch (IOException e) {
					throw new RuntimeException("Close the collector fail.", e);
				}
			}
		}

		public void close() throws IOException {
			if (zipOutputStream != null) {
				zipOutputStream.close();
				zipOutputStream = null;
			}
		}
	}

	AkStream(AkUtils.AkMeta akMeta) {
		this.akMeta = akMeta;
	}

	AkReader getReader(InputStream inputStream) throws IOException {
		return new AkReader(inputStream);
	}

	AkWriter getWriter(OutputStream outputStream) throws IOException {
		return new AkWriter(outputStream);
	}

	private static AkUtils.AkMeta initialMeta(FilePath filePath) throws IOException {

		AkUtils.AkMeta meta = null;
		ZipEntry entry;
		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(filePath.getFileSystem().open(filePath.getPathStr())))) {
			while ((entry = zis.getNextEntry()) != null) {
				if (entry.getName().equalsIgnoreCase(AkUtils.META_FILE)) {
					meta = JsonConverter.fromJson(
						IOUtils.toString(zis, StandardCharsets.UTF_8),
						AkUtils.AkMeta.class
					);
					break;
				}
			}
		}

		return meta;
	}

	private static void writeMeta2Stream(
		AkUtils.AkMeta meta,
		ZipOutputStream zos) throws IOException {

		zos.putNextEntry(new ZipEntry(AkUtils.META_FILE));
		zos.write(JsonConverter.toJson(meta).getBytes());
	}
}
