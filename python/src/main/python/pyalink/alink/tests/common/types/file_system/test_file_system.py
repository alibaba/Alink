import unittest

import pytest

from pyalink.alink import *


def print_value_and_type(v):
    print(type(v), v)


class TestFileSystem(unittest.TestCase):

    def test_base_file_system(self):
        from pyalink.alink.common.types.file_system.file_system import BaseFileSystem
        self.assertFalse(BaseFileSystem.isFileSystem(Params()))
        self.assertTrue(BaseFileSystem.isFileSystem(Params().set("fsUri", "hdfs://xxx:9000").set("ioName", "hadoop")))
        hfs = BaseFileSystem.of(Params().set("fsUri", "hdfs://xxx:9000").set("ioName", "hadoop"))
        self.assertEquals(type(hfs), HadoopFileSystem)

    def test_local_file_system(self):
        lfs = LocalFileSystem()
        print_value_and_type(lfs)
        schema = lfs.getSchema()
        print_value_and_type(schema)
        self.assertEquals(schema, 'file')
        self.assertIsNotNone(lfs.getWorkingDirectory())
        self.assertIsNotNone(lfs.getHomeDirectory())
        self.assertIsNotNone(lfs.getUri())
        self.assertFalse(lfs.isDistributedFS())
        print_value_and_type(lfs.getKind())

        print(lfs.listDirectories(lfs.getWorkingDirectory()))
        print(lfs.listFiles(lfs.getWorkingDirectory()))

        homeDirectory = lfs.getHomeDirectory()
        print_value_and_type(homeDirectory)
        uri = lfs.getUri()
        print_value_and_type(uri)
        filepath = Path(homeDirectory, "data.csv")
        print_value_and_type(filepath)

        filepath = Path(homeDirectory, "data_write.csv")
        fos = lfs.create(filepath, True)
        fos.write(b'abcdef')
        fos.flush()
        fos.close()

        self.assertTrue(lfs.exists(FilePath(filepath).getPathStr()))

        fis = lfs.open(filepath)
        print(fis.read())
        (numBytesRead, b) = fis.read(10000, 0)
        print(numBytesRead, b)

    @pytest.mark.skip()
    def test_hadoop_file_system(self):
        hadoop_fs = HadoopFileSystem("2.8.3", "hdfs://xxx:9000")
        print_value_and_type(hadoop_fs)
        schema = hadoop_fs.getSchema()
        print_value_and_type(schema)
        homeDirectory = hadoop_fs.getHomeDirectory()
        print_value_and_type(homeDirectory)
        uri = hadoop_fs.getUri()
        print_value_and_type(uri)
        filepath = Path(homeDirectory, "data.csv")
        print_value_and_type(filepath)

        filepath = Path(homeDirectory, "data_write.csv")
        fos = hadoop_fs.create(filepath, True)
        fos.write(b'abcdef')
        fos.flush()
        fos.close()

        fis = hadoop_fs.open(filepath)
        print(fis.read())
        (numBytesRead, b) = fis.read(10000, 0)
        print(numBytesRead, b)

    @pytest.mark.skip()
    def test_oss_file_system(self):
        oss_fs = OssFileSystem("3.4.1", "xxx", "xxx", "xxx", "xxx")
        print_value_and_type(oss_fs)
        schema = oss_fs.getSchema()
        print_value_and_type(schema)
        homeDirectory = oss_fs.getHomeDirectory()
        print_value_and_type(homeDirectory)
        uri = oss_fs.getUri()
        print_value_and_type(uri)
        filepath = Path(homeDirectory, "jiqi-temp/data.csv")
        print_value_and_type(filepath)

        filepath = Path(homeDirectory, "jiqi-temp/data_write.csv")
        fos = oss_fs.create(filepath, True)
        fos.write(b'abcdef')
        fos.flush()
        fos.close()

        fis = oss_fs.open(filepath)
        print(fis.read())
        (numBytesRead, b) = fis.read(10000, 0)
        print(numBytesRead, b)
