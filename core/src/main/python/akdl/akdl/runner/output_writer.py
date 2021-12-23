import struct

import crc32c as crc32c
import numpy as np
import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

__all__ = [
    'get_fake_output_writer',
    'OutputWriter', 'DirectOutputWriter'
]

from flink_ml_framework.java_file import JavaFile


def fake_output_writer_op(input_list):
    return tf.print(input_list), tf.no_op


class OutputWriter:
    def __init__(self, output_writer_op):
        self._write_feed = tf.placeholder(dtype=tf.string)
        self._write_op, self._close_op = output_writer_op([self._write_feed])
        self._sess: tf.Session = tf.Session()

    def close(self):
        self._sess.run(self._close_op)
        self._sess.close()

    def write(self, example: tf.train.Example):
        self._sess.run(self._write_op, feed_dict={self._write_feed: example.SerializeToString()})


class DirectOutputWriter:
    def __init__(self, from_java: str, to_java: str):
        self.java_file = JavaFile(from_java, to_java)

    def close(self):
        pass

    @staticmethod
    def masked_crc(data: bytes) -> bytes:
        """CRC checksum."""
        mask = 0xa282ead8
        crc = crc32c.crc32(data)
        masked = (((crc >> 15) | (crc << 17)) + mask) & 0xffffffff
        masked = np.uint32(masked)
        masked_bytes = struct.pack("<I", masked)
        return masked_bytes

    def write(self, example: tf.train.Example):
        record = example.SerializeToString()
        length = len(record)
        length_bytes = struct.pack("<Q", length)
        self.java_file.write(length_bytes, 8)
        self.java_file.write(self.masked_crc(length_bytes), 4)
        self.java_file.write(record, length)
        self.java_file.write(self.masked_crc(record), 4)


def get_fake_output_writer():
    return OutputWriter(fake_output_writer_op)
