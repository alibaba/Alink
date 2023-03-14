package com.alibaba.alink.operator.common.io.reader;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.FileSystemUtils;
import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat.InputSplitOpenThread;
import com.alibaba.alink.operator.common.io.csv.GenericCsvInputFormatBeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FSFileSplitReader implements FileSplitReader, AutoCloseable {
	private final Path readerPath;
	private final BaseFileSystem fs;
	protected static final long openTimeout = 300000;

	private FSCsvInputFormat inputFormat = null;

	private transient Path filePath;
	private transient FileInputSplit split;
	private transient FSDataInputStream stream;

	public FSFileSplitReader(FilePath filePath) {
		this.readerPath = filePath.getPath();
		this.fs = filePath.getFileSystem();
	}

	@Override
	public void open(InputSplit split) throws IOException {
		long splitStart = ((FileInputSplit) split).getStart();
		this.reopen(split, splitStart);
	}

	@Override
	public void reopen(InputSplit split, long splitStart) throws IOException {
		this.split = (FileInputSplit) split;
		long splitLength = this.split.getLength();

		//System.out.println(
		//	String.valueOf(split.getSplitNumber()) + " opening the Input Split " + this.split.getPath() + " ["
		//		+ splitStart + "," + splitLength + "]: ");

		final InputSplitOpenThread isot = new InputSplitOpenThread(this.split, this.openTimeout, this.fs);
		isot.start();
		try {
			this.stream = isot.waitForCompletion();
		} catch (Throwable t) {
			throw new AkUnclassifiedErrorException("Error opening the Input Split " + this.split.getPath() +
				" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}
		this.filePath = this.split.getPath();
		if (splitStart > 0) {
			this.stream.seek(splitStart);
		}

	}

	@Override
	public void close() throws IOException {
		if (this.stream != null) {
			// close input stream
			this.stream.close();
			stream = null;
		}
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return this.stream.read(b, off, len);
	}

	@Override
	public long getFileLength() {
		try {
			FileStatus stat = fs.getFileStatus(readerPath);
			return stat.getLen();
		} catch (IOException e) {
			return 0;
		}	

	}

	@Override
	public long getSplitLength() {
		return split.getLength();
	}

	@Override
	public long getSplitStart() {
		return split.getStart();
	}

	public String getSplit() {return this.split.toString();}

	/**
	 * The ending position is get from file status because FileInputSplit is different from CsvFileInput and doesn't
	 * have a variable that represent ending position of this split.
	 *
	 * @return return file length, if fail to get file length then return 0
	 * @see com.alibaba.alink.operator.common.io.csv.CsvFileInputSplit#end
	 */
	@Override
	public long getSplitEnd() {
		try {
			FileStatus stat = fs.getFileStatus(filePath);
			return stat.getLen();
		} catch (IOException e) {
			return 0;
		}
	}

	@Override
	public long getSplitNumber() {
		return split.getSplitNumber();
	}

	public Path getFilePath() {
		return readerPath;
	}

	public BaseFileSystem getFs() {
		return fs;
	}

	@Override
	public FSCsvInputFormat getInputFormat(String lineDelim, boolean ignoreFirstLine, Character quoteChar) {
		if (inputFormat == null) {
			inputFormat = new FSCsvInputFormat(this, lineDelim, ignoreFirstLine,
				quoteChar);
		}
		return inputFormat;
	}

	@Override
	public FSCsvInputFormat convertFileSplitToInputFormat(String lineDelim, boolean ignoreFirstLine,
														  Character quoteChar) {
		return new FSCsvSplitInputFormat(this, lineDelim, ignoreFirstLine, quoteChar);
	}

	@Override
	public InputSplit convertStringToSplitObject(String splitStr) {

		return FSCsvSplitInputFormat.fromString(splitStr);
	}

	public static class FSCsvInputFormat extends GenericCsvInputFormatBeta <FileInputSplit> {
		// -------------------------------------- Constants -------------------------------------------

		private static final Logger LOG = LoggerFactory.getLogger(FSCsvInputFormat.class);

		private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

		/**
		 * The timeout (in milliseconds) to wait for a filesystem stream to respond.
		 */
		private static long DEFAULT_OPENING_TIMEOUT;

		static {
			initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
		}

		/**
		 * Initialize defaults for input format. Needs to be a static method because it is configured for local
		 * cluster execution.
		 *
		 * @param configuration The configuration to load defaults from
		 */
		private static void initDefaultsFromConfiguration(Configuration configuration) {
			final long to = configuration.getLong(ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
				ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
			if (to < 0) {
				LOG.error("Invalid timeout value for filesystem stream opening: " + to + ". Using default value of " +
					ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
				DEFAULT_OPENING_TIMEOUT = ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
			} else if (to == 0) {
				DEFAULT_OPENING_TIMEOUT = 300000; // 5 minutes
			} else {
				DEFAULT_OPENING_TIMEOUT = to;
			}
		}

		// --------------------------------------------------------------------------------------------
		//  The configuration parameters. Configured on the instance and serialized to be shipped.
		// --------------------------------------------------------------------------------------------

		private final Path filePath;
		/**
		 * The minimal split size, set by the configure() method.
		 */
		protected long minSplitSize = 0;

		/**
		 * The desired number of splits, as set by the configure() method.
		 */
		protected int numSplits = -1;

		/**
		 * Stream opening timeout.
		 */
		protected long openTimeout = DEFAULT_OPENING_TIMEOUT;

		private long offset = -1;

		/**
		 * The flag to specify whether recursive traversal of the input directory
		 * structure is enabled.
		 */
		protected boolean enumerateNestedFiles = true;

		/**
		 * Files filter for determining what files/directories should be included.
		 */
		private FilePathFilter filesFilter = new GlobFilePathFilter();

		private final BaseFileSystem <?> fs;

		// --------------------------------------------------------------------------------------------
		//  Constructors
		// --------------------------------------------------------------------------------------------

		public FSCsvInputFormat(FSFileSplitReader reader,
								String lineDelim, boolean ignoreFirstLine) {
			super(reader, lineDelim, ignoreFirstLine);
			this.fs = ((FSFileSplitReader) reader).getFs();
			this.filePath = ((FSFileSplitReader) reader).getFilePath();
		}

		public FSCsvInputFormat(FSFileSplitReader reader,
								String lineDelim, boolean ignoreFirstLine,
								Character quoteChar) {
			super(reader, lineDelim, ignoreFirstLine, quoteChar);
			this.fs = ((FSFileSplitReader) reader).getFs();
			this.filePath = ((FSFileSplitReader) reader).getFilePath();
		}

		public FSCsvInputFormat(FSFileSplitReader reader,
								String lineDelim, boolean ignoreFirstLine,
								boolean unsplitable, Character quoteChar) {
			super(reader, lineDelim, ignoreFirstLine, unsplitable, quoteChar);
			this.fs = ((FSFileSplitReader) reader).getFs();
			this.filePath = ((FSFileSplitReader) reader).getFilePath();
		}

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.open(split);
			if (split.getStart() > 0 || ignoreFirstLine) {
				if (!readLine()) {
					// if the first partial record already pushes the stream over
					// the limit of our split, then no record starts within this split
					setEnd(true);
				}

			} else {
				fillBuffer(0);
			}
		}

		public void openWithoutSkipLine(FileInputSplit split) throws IOException {
			super.open(split);
		}

		@Override
		public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			if (minNumSplits < 1) {
				throw new AkIllegalArgumentException("Number of input splits has to be at least 1.");
			}

			// take the desired number of splits into account
			minNumSplits = Math.max(minNumSplits, this.numSplits);

			final List <FileInputSplit> inputSplits = new ArrayList <FileInputSplit>(minNumSplits);

			// get all the files that are involved in the splits
			List <FileStatus> files = new ArrayList <>();
			long totalLength = 0;

			if (filePath != null) {
				final FileSystem fs = FileSystemUtils.getFlinkFileSystem(this.fs, filePath.toString());
				final FileStatus pathFile = fs.getFileStatus(filePath);

				if (pathFile.isDir()) {
					totalLength += addFilesInDir(filePath, files, true);
				} else {
					files.add(pathFile);
					totalLength += pathFile.getLen();
				}
			}

			// returns if unsplittable
			if (unsplittable) {
				int splitNum = 0;
				for (final FileStatus file : files) {
					final FileSystem fs = FileSystemUtils.getFlinkFileSystem(this.fs, file.getPath().toString());
					final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
					Set <String> hosts = new HashSet <String>();
					for (BlockLocation block : blocks) {
						hosts.addAll(Arrays.asList(block.getHosts()));
					}
					long len = file.getLen();
					//len = READ_WHOLE_SPLIT_FLAG;
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, len,
						hosts.toArray(new String[hosts.size()]));
					inputSplits.add(fis);
				}
				return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
			}

			final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

			// now that we have the files, generate the splits
			int splitNum = 0;
			for (final FileStatus file : files) {

				final FileSystem fs = FileSystemUtils.getFlinkFileSystem(this.fs, file.getPath().toString());
				final long len = file.getLen();
				final long blockSize = file.getBlockSize();

				final long minSplitSize;
				if (this.minSplitSize <= blockSize) {
					minSplitSize = this.minSplitSize;
				} else {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " +
							blockSize + ". Decreasing minimal split size to block size.");
					}
					minSplitSize = blockSize;
				}

				final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
				final long halfSplit = splitSize >>> 1;

				final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
				if (len > 0) {

					// get the block locations and make sure they are in order with respect to their offset
					final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
					Arrays.sort(blocks);

					long bytesUnassigned = len;
					long position = 0;

					int blockIndex = 0;

					while (bytesUnassigned > maxBytesForLastSplit) {
						// get the block containing the majority of the data
						blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
						// create a new split
						FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
							blocks[blockIndex].getHosts());
						inputSplits.add(fis);

						// adjust the positions
						position += splitSize;
						bytesUnassigned -= splitSize;
					}

					// assign the last split
					if (bytesUnassigned > 0) {
						blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
						final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
							bytesUnassigned, blocks[blockIndex].getHosts());
						inputSplits.add(fis);
					}
				} else {
					// special case with a file of zero bytes size
					final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
					String[] hosts;
					if (blocks.length > 0) {
						hosts = blocks[0].getHosts();
					} else {
						hosts = new String[0];
					}
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
					inputSplits.add(fis);
				}
			}

			return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
		}

		private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
			// go over all indexes after the startIndex
			for (int i = startIndex; i < blocks.length; i++) {
				long blockStart = blocks[i].getOffset();
				long blockEnd = blockStart + blocks[i].getLength();

				if (offset >= blockStart && offset < blockEnd) {
					// got the block where the split starts
					// check if the next block contains more than this one does
					if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
						return i + 1;
					} else {
						return i;
					}
				}
			}
			throw new AkIllegalArgumentException("The given offset is not contained in the any block.");
		}

		/**
		 * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
		 *
		 * @return the total length of accepted files.
		 */
		private long addFilesInDir(Path path, List <FileStatus> files, boolean logExcludedFiles)
			throws IOException {
			final FileSystem fs = FileSystemUtils.getFlinkFileSystem(this.fs, path.toString());

			long length = 0;

			for (FileStatus dir : fs.listStatus(path)) {
				if (dir.isDir()) {
					length += addFilesInDir(dir.getPath(), files, logExcludedFiles);
				} else {
					if (acceptFile(dir)) {
						files.add(dir);
						length += dir.getLen();
					} else {
						if (logExcludedFiles && LOG.isDebugEnabled()) {
							LOG.debug(
								"Directory " + dir.getPath().toString()
									+ " did not pass the file-filter and is excluded"
									+ ".");
						}
					}
				}
			}
			return length;
		}

		private boolean acceptFile(FileStatus fileStatus) {
			final String name = fileStatus.getPath().getName();
			final FilePathFilter filesFilter = new GlobFilePathFilter();
			return !name.startsWith("_")
				&& !name.startsWith(".")
				&& !filesFilter.filterPath(fileStatus.getPath());
		}
	}

	public static class FSCsvSplitInputFormat extends FSCsvInputFormat {

		public FSCsvSplitInputFormat(FSFileSplitReader reader, String lineDelim, boolean ignoreFirstLine,
									 Character quoteChar) {
			super(reader, lineDelim, ignoreFirstLine, false, quoteChar);
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.openWithoutSkipLine(split);
		}

		/**
		 * This format is used to scan each split. It only returns one record then set end flag to true.
		 * The record includes quote character num and a string that stores variable used to rebuild the split.
		 *
		 * @param record Object that may be reused.
		 * @return quote count, split status and split serialized result
		 * @throws IOException
		 */
		@Override
		public Row nextRecord(Row record) throws IOException {
			long quoteNum = QuoteUtil.analyzeSplit(this.reader, quoteCharacter);

			StringBuilder sbd = new StringBuilder();
			sbd.append(this.currentSplit.toString());
			sbd.append("[");
			for (String host : this.currentSplit.getHostnames()) {
				sbd.append(host).append(";");
			}
			sbd.append("]");

			this.setEnd(true);
			this.reader.close();

			return Row.of(quoteNum, this.reader.getSplitNumber(), sbd.toString());
		}

		public static FileInputSplit fromString(String splitStr) {
			int leftBracketsPosFirst = splitStr.indexOf("[");
			int rightBracketsPosFirst = splitStr.indexOf("]");
			int leftBracketsPosSecond = splitStr.indexOf("[", leftBracketsPosFirst + 1);
			int rightBracketsPosSecond = splitStr.indexOf("]", rightBracketsPosFirst + 1);
			int filePathEndPos = splitStr.lastIndexOf(":");
			int plusPos = splitStr.lastIndexOf("+");

			int num = Integer.valueOf(splitStr.substring(leftBracketsPosFirst + 1, rightBracketsPosFirst));
			String filePath = splitStr.substring(rightBracketsPosFirst + 2, filePathEndPos);
			long start = Long.valueOf(splitStr.substring(filePathEndPos + 1, plusPos));
			long length = Long.valueOf(splitStr.substring(plusPos + 1, leftBracketsPosSecond));
			String[] hosts = splitStr.substring(leftBracketsPosSecond + 1, rightBracketsPosSecond).split(";");
			Path path = new Path(filePath);

			return new FileInputSplit(num, path, start, length, hosts);
		}
	}
}
