package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.io.HasIoName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Test file system using local file system.
 * <p>
 * Methods that using distribute file system need to test manually.
 */
public class BaseFileSystemTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void getSchema() {
		Assert.assertEquals("file", new LocalFileSystem().getSchema());
	}

	@Test
	public void getWorkingDirectory() {
		Assert.assertNotNull(new LocalFileSystem().getWorkingDirectory());
	}

	@Test
	public void getHomeDirectory() {
		Assert.assertNotNull(new LocalFileSystem().getHomeDirectory());
	}

	@Test
	public void getUri() {
		Assert.assertNotNull(new LocalFileSystem().getUri());
	}

	@Test
	public void isDistributedFS() {
		Assert.assertFalse(new LocalFileSystem().isDistributedFS());
	}

	@Test
	public void getKind() {
		Assert.assertEquals(FileSystemKind.FILE_SYSTEM, new LocalFileSystem().getKind());
	}

	@Test
	public void exists() throws IOException {
		final String sign = "file_exists";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));

		Assert.assertFalse(file.getFileSystem().exists(file.getPathStr()));
	}

	@Test
	public void mkdirs() throws IOException {
		final String sign = "file_mkdir";

		BaseFileSystem<?> fileSystem = new LocalFileSystem();
		String path = folder.getRoot().toPath().toString() + "/" + sign;

		fileSystem.mkdirs(path);
		Assert.assertTrue(fileSystem.getFileStatus(path).isDir());
	}

	@Test
	public void of() {
		Assert.assertTrue(BaseFileSystem.of(new Params().set(HasIoName.IO_NAME, "local")) instanceof LocalFileSystem);
	}

	@Test
	public void isFileSystem() {
		Assert.assertTrue(BaseFileSystem.isFileSystem(new Params().set(HasIoName.IO_NAME, "local")));
	}

	@Test
	public void getParams() {
		Assert.assertNotNull(new LocalFileSystem().getParams());
	}

	@Test
	public void getFileStatus() throws IOException {
		final String sign = "file_status";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign), new LocalFileSystem());

		try {
			file.getFileSystem().getFileStatus(file.getPathStr());
			Assert.fail("file not exist. It should throw exception.");
		} catch (IOException e) {
			// pass
		}

		writeFile(file, sign);
		FileStatus fileStatus = file.getFileSystem().getFileStatus(file.getPathStr());
		Assert.assertFalse(fileStatus.isDir());
	}

	@Test
	public void open() throws IOException {
		final String sign = "file_open";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign), new LocalFileSystem());

		writeFile(file, sign);
		byte[] buffer = new byte[1024];
		int len = file.getFileSystem().open(file.getPathStr(), 1024).read(buffer);

		Assert.assertEquals(new String(buffer, 0, len), sign);
	}

	@Test
	public void testOpen() throws IOException {
		final String sign = "file_open";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));

		writeFile(file, sign);
		byte[] buffer = new byte[1024];
		int len = file.getFileSystem().open(file.getPathStr()).read(buffer);

		Assert.assertEquals(new String(buffer, 0, len), sign);
	}

	@Test
	public void listStatus() throws IOException {
		final String sign = "folder_list_status";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString()));

		writeFile(new FilePath(new Path(folder.getRoot().toPath().toString(), sign)), sign);
		Assert.assertEquals(file.getFileSystem().listStatus(file.getPathStr()).length, 1);
	}

	@Test
	public void delete() throws IOException {

		final String sign = "file_delete";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));

		writeFile(file, sign);
		Assert.assertTrue(file.getFileSystem().exists(file.getPathStr()));
		file.getFileSystem().delete(file.getPathStr(), false);
		Assert.assertFalse(file.getFileSystem().exists(file.getPathStr()));
	}

	private interface FunctionOutputStream {
		OutputStream apply(FilePath filePath) throws IOException;
	}

	private static void testCreate(TemporaryFolder folder, FunctionOutputStream streamFunction) throws IOException {
		final String sign = "file_create";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));
		OutputStream outputStream = null;

		try {
			outputStream = new BufferedOutputStream(
				streamFunction.apply(file)
			);

			outputStream.write(sign.getBytes());
		} finally {
			if (outputStream != null) {
				outputStream.close();
			}
		}

		InputStream inputStream = null;

		try {
			inputStream = new BufferedInputStream(
				file.getFileSystem().open(file.getPathStr())
			);

			byte[] buffer = new byte[1024];
			int len = inputStream.read(buffer);
			Assert.assertEquals(new String(buffer, 0, len), sign);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void create() throws IOException {
		testCreate(folder, x -> x.getFileSystem().create(x.getPathStr(), false, 0, (short) 0, 0L));
	}

	@Test
	public void testCreate() throws IOException {
		testCreate(folder, x -> x.getFileSystem().create(x.getPathStr(), false));
	}

	@Test
	public void testCreate1() throws IOException {
		testCreate(folder, x -> x.getFileSystem().create(x.getPathStr(), FileSystem.WriteMode.NO_OVERWRITE));
	}

	@Test
	public void rename() throws IOException {
		final String sign = "file_rename";

		FilePath file = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));

		writeFile(file, sign);
		file.getFileSystem().rename(file.getPathStr(), file.getPathStr() + "_new");
		Assert.assertTrue(file.getFileSystem().exists(file.getPathStr() + "_new"));
	}

	@Test
	public void initOutPathLocalFS() throws IOException {
		final String sign = "file_init_output_path";

		FilePath folderWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));
		FilePath fileWrite = new FilePath(new Path(folderWrite.getPathStr(), "1"), folderWrite.getFileSystem());
		folderWrite.getFileSystem().mkdirs(folderWrite.getPathStr());
		writeFile(fileWrite, sign);

		folderWrite.getFileSystem().initOutPathLocalFS(folderWrite.getPathStr(), FileSystem.WriteMode.OVERWRITE, true);

		Assert.assertTrue(fileWrite.getFileSystem().exists(fileWrite.getPathStr()));
	}

	@Test
	public void listFiles() throws IOException {
		final String sign = "file_list";

		FilePath folderWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));
		FilePath fileWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), "1"), folderWrite.getFileSystem());
		folderWrite.getFileSystem().mkdirs(folderWrite.getPathStr());
		writeFile(fileWrite, sign);

		Assert.assertEquals(1, folderWrite.getFileSystem().listFiles(folder.getRoot().toPath().toString()).size());
	}

	@Test
	public void listDirectories() throws IOException {
		final String sign = "file_dir";

		FilePath folderWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));
		FilePath fileWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), "1"), folderWrite.getFileSystem());
		folderWrite.getFileSystem().mkdirs(folderWrite.getPathStr());
		writeFile(fileWrite, sign);

		Assert.assertEquals(1, folderWrite.getFileSystem().listDirectories(folder.getRoot().toPath().toString()).size());
	}

	@Test
	public void getFileBlockLocations() throws IOException {
		final String sign = "file_blocal_locations";

		FilePath fileWrite = new FilePath(new Path(folder.getRoot().toPath().toString(), sign));

		writeFile(fileWrite, sign);

		Assert.assertEquals(1, fileWrite.getFileSystem().getFileBlockLocations(
			fileWrite.getFileSystem().getFileStatus(fileWrite.getPathStr()), 0, 1
		).length);
	}


	private void writeFile(FilePath path, String content) throws IOException {
		FSDataOutputStream outputStream = null;
		try {
			outputStream = path.getFileSystem().create(path.getPath(), FileSystem.WriteMode.OVERWRITE);
			outputStream.write(content.getBytes());
		} finally {
			if (outputStream != null) {
				outputStream.close();
			}
		}
	}
}