package com.alibaba.alink.operator.common.regression.isotonicReg;

import org.apache.flink.util.Preconditions;

/**
 * This class helps the byte array blocks operate like a linked list.
 * The index of the previous and forward neighbors is written in an int array,
 * and the point points the current index.
 * Visit nextIndex to get the forward neighbor.
 * Visit prevIndex to get the previous neighbor.
 * Change the forward neighbor information of the previous one and
 * change the previous neighbor information of the forward one will help remove the current node.
 */
class IteratorArray {
    private int[] prevIndex;
    private int[] nextIndex;
    private int point;

    IteratorArray(int length, int point) {
        Preconditions.checkArgument(length > 0, "length must be positive!");
        Preconditions.checkArgument(point < length && point >= 0, "point out of range!");
        this.prevIndex = new int[length];
        this.nextIndex = new int[length];
        for (int i = 0; i < length; i++) {
            this.prevIndex[i] = i - 1;
            this.nextIndex[i] = i + 1;
        }
        this.nextIndex[length - 1] = -1;
        this.point = point;
    }

    //judge whether the current node has previous.
    boolean hasPrevious() {
        return this.prevIndex[point] != -1;
    }

    //judge whether the current node has next.
    boolean hasNext() {
        return this.nextIndex[point] != -1;
    }

    //move the point points to the next node.
    void advance() {
        point = this.nextIndex[point];
    }

    //move the point points to the previous node.
    void retreat() {
        point = this.prevIndex[point];
    }

    //remove the current node and then move the point points to the previous node.
    void removeCurrentAndRetreat() {
        int pre = this.prevIndex[point];
        int next = this.nextIndex[point];
        if (next != -1) {
            this.nextIndex[pre] = next;
            this.prevIndex[next] = pre;
            point = pre;
        } else {
            this.nextIndex[pre] = -1;
            point = pre;
        }
    }

    //set the point to 0.
    void initializePoint() {
        this.point = 0;
    }

    //get the current point.
    int getPoint() {
        return this.point;
    }
}
