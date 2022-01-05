package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Objects;

public class ArchivesUtilsTest extends AlinkTestBase {
	@Test
	public void testDownloadDecompressToDirectory() {
		File tempDir = PythonFileUtils.createTempDir(null).toFile();
		ArchivesUtils.downloadDecompressToDirectory("res:///ckpts.zip", tempDir);
		// contents of this compressed file is a folder named `ckpts`
		File subDir = new File(tempDir, "ckpts");
		Assert.assertTrue(subDir.exists());
		Assert.assertTrue(subDir.isDirectory());
		Assert.assertTrue(Objects.requireNonNull(subDir.listFiles()).length > 0);
	}
}
