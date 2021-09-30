/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

package com.alibaba.flink.ml.tensorflow2.data;

import org.apache.hadoop.util.PureJavaCrc32C;

import java.util.zip.Checksum;

public class Crc32C implements Checksum {
  private static final int MASK_DELTA = 0xa282ead8;
  private PureJavaCrc32C crc32C;

  public static int maskedCrc32c(byte[] data) {
    return maskedCrc32c(data, 0, data.length);
  }

  public static int maskedCrc32c(byte[] data, int offset, int length) {
    Crc32C crc32c = new Crc32C();
    crc32c.update(data, offset, length);
    return crc32c.getMaskedValue();
  }

  /**
   * Return a masked representation of crc.
   * <p>
   *  Motivation: it is problematic to compute the CRC of a string that
   *  contains embedded CRCs.  Therefore we recommend that CRCs stored
   *  somewhere (e.g., in files) should be masked before being stored.
   * </p>
   * @param crc CRC
   * @return masked CRC
   */
  public static int mask(int crc) {
    // Rotate right by 15 bits and add a constant.
    return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
  }

  /**
   * Return the crc whose masked representation is masked_crc.
   * @param maskedCrc masked CRC
   * @return crc whose masked representation is masked_crc
   */
  public static int unmask(int maskedCrc) {
    int rot = maskedCrc - MASK_DELTA;
    return ((rot >>> 17) | (rot << 15));
  }

  public Crc32C() {
    crc32C = new PureJavaCrc32C();
  }

  public int getMaskedValue() {
    return mask(getIntValue());
  }

  public int getIntValue() {
    return (int) getValue();
  }

  @Override public void update(int b) {
    crc32C.update(b);
  }

  @Override public void update(byte[] b, int off, int len) {
    crc32C.update(b, off, len);
  }

  @Override public long getValue() {
    return crc32C.getValue();
  }

  @Override public void reset() {
    crc32C.reset();
  }
}
