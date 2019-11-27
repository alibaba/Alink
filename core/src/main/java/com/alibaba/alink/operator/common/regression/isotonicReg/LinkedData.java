package com.alibaba.alink.operator.common.regression.isotonicReg;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is a wrapper of data and the linked-list-link operations.
 */
public class LinkedData {
    private final ByteBuffer buffer;
    private final IteratorArray iteratorArray;

    public LinkedData(byte[] data) {
        Preconditions.checkArgument(data.length % 24 == 0, "Invalid data!");
        iteratorArray = new IteratorArray(data.length / 24, 0);
        buffer = ByteBuffer.wrap(data);
    }

    public LinkedData(List<Tuple3<Double, Double, Double>> listData) {
        int length = listData.size();
        if (length != 0) {
            iteratorArray = new IteratorArray(length, 0);
            buffer = ByteBuffer.wrap(new byte[length * 24]);
            for (Tuple3<Double, Double, Double> t : listData) {
                buffer.putFloat((float) (t.f0 * t.f2));
                buffer.putDouble(t.f1);
                buffer.putDouble(t.f1);
                buffer.putFloat(t.f2.floatValue());
            }
        } else {
            buffer = null;
            iteratorArray = null;
        }
    }

    //put data into the current block.
    public void putData(float f0, double f1, double f2, float f3) {
        int currentIndex = this.iteratorArray.getPoint() * 24;
        buffer.putFloat(currentIndex, f0);
        buffer.putDouble(currentIndex + 4, f1);
        buffer.putDouble(currentIndex + 12, f2);
        buffer.putFloat(currentIndex + 20, f3);
    }

    private void putData(int currentpoint, Tuple4<Float, Double, Double, Float> tuple4) {
        int currentIndex = currentpoint * 24;
        buffer.putFloat(currentIndex, tuple4.f0);
        buffer.putDouble(currentIndex + 4, tuple4.f1);
        buffer.putDouble(currentIndex + 12, tuple4.f2);
        buffer.putFloat(currentIndex + 20, tuple4.f3);
    }

    //get data of the current block.
    public Tuple4<Float, Double, Double, Float> getData() {
        int currentIndex = this.iteratorArray.getPoint() * 24;
        return Tuple4.of(buffer.getFloat(currentIndex),
                buffer.getDouble(currentIndex + 4),
                buffer.getDouble(currentIndex + 12),
                buffer.getFloat(currentIndex + 20));
    }

    //get the byte array.
    public byte[] getByteArray() {
        return buffer == null ? null : buffer.array();
    }

    //compact the byte array and finally return the number of non empty blocks.
    public int compact() {
        int tempPoint = 0;
        this.iteratorArray.initializePoint();
        while (this.iteratorArray.getPoint() != -1) {
            if (tempPoint != this.iteratorArray.getPoint()) {
                Tuple4<Float, Double, Double, Float> nextBlock = getData();
                putData(tempPoint, nextBlock);
            }
            tempPoint++;
            advance();
        }
        return tempPoint;
    }

    // linked list operation methods.
    public boolean hasPrevious() {
        return iteratorArray.hasPrevious();
    }

    public boolean hasNext() {
        return iteratorArray.hasNext();
    }

    //before advance, need to check hasNext.
    public void advance() {
        iteratorArray.advance();
    }

    //before retreat, need to check hasPrevious.
    public void retreat() {
        iteratorArray.retreat();
    }

    public void removeCurrentAndRetreat() {
        iteratorArray.removeCurrentAndRetreat();
    }


}
