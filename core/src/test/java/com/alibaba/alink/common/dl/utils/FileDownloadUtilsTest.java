package com.alibaba.alink.common.dl.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class FileDownloadUtilsTest {

	@Test
	public void testDownloadFile() {
		File tempDir = PythonFileUtils.createTempDir(null);
		File targetFile = new File(tempDir, "data.zip");
		FileDownloadUtils.downloadFile("res:///ckpts.zip", targetFile);
		Assert.assertTrue(targetFile.exists());
		Assert.assertTrue(targetFile.length() > 0);
	}

	@Test
	public void testDownloadFile2() {
		File tempDir = PythonFileUtils.createTempDir(null);
		FileDownloadUtils.downloadFile("res:///ckpts.zip", tempDir, "data.zip");
		File targetFile = new File(tempDir, "data.zip");
		Assert.assertTrue(targetFile.exists());
		Assert.assertTrue(targetFile.length() > 0);
	}

	@Test
	public void testDownloadFileToDirectory() {
		File tempDir = PythonFileUtils.createTempDir(null);
		String fn = FileDownloadUtils.downloadFileToDirectory("res:///ckpts.zip", tempDir);
		Assert.assertEquals(fn, "ckpts.zip");
		File targetFile = new File(tempDir, fn);
		Assert.assertTrue(targetFile.exists());
		Assert.assertTrue(targetFile.length() > 0);
	}

}
