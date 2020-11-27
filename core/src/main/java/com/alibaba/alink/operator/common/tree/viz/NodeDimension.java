/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.tree.viz;

import java.awt.*;

public class NodeDimension {

	public static final int DEFAULT_NODE_HIGH = 100;
	public static final int DEFAULT_NODE_WIDTH = 100;
	public static final int DEFAULT_NODE_WIDTH_WITHOUT_VALIDATION = 200;
	public static final int DEFAULT_WIDTH_SPACE = 7;
	public static final int DEFAULT_HEIGHT_SPACE = 70;
	public static final int NODE_HEIGHT_STEP = DEFAULT_NODE_HIGH / 10;
	public static final int SPACE_HEIGHT_STEP = DEFAULT_HEIGHT_SPACE / 10;
	public static final int MAX_NODE_HEIGHT = DEFAULT_NODE_HIGH * 2;
	public static final int MAX_SPACE_HEIGHT = DEFAULT_HEIGHT_SPACE * 2;
	public static final int MIN_NODE_HEIGHT = DEFAULT_NODE_HIGH / 10;
	public static final int MIN_SPACE_HEIGHT = DEFAULT_HEIGHT_SPACE / 10;
	//control size
	public int nodeHigh = -1;
	public int nodeWidth = -1;
	public int widthSpace = -1;
	public int heightSpace = -1;
	public boolean isValidate = true;
	public double nodeRatio;
	public double spaceRatio;
	public static final int[] FONTSIZESET = {4, 4, 9, 9, 9, 10, 10, 11, 11, 12};
	public static final int[] EDGEFONTSIZESET = {4, 5, 6, 8, 9, 10, 10, 11, 11, 12};
	public static final int[] EDGEMAXWIDTH = {4, 4, 4, 4, 5, 6, 7, 8, 9, 10};

	public NodeDimension() {
		init();
	}

	public NodeDimension(boolean isValidate) {
		this.isValidate = isValidate;
		init();
	}

	public void init() {
		this.nodeHigh = this.DEFAULT_NODE_HIGH;
		if (this.isValidate) {
			this.nodeWidth = this.DEFAULT_NODE_WIDTH_WITHOUT_VALIDATION;
		} else {
			this.nodeWidth = this.DEFAULT_NODE_WIDTH;
		}
		this.heightSpace = this.DEFAULT_HEIGHT_SPACE;
		this.widthSpace = this.DEFAULT_WIDTH_SPACE;

		this.nodeRatio = this.nodeWidth / this.nodeHigh;
		this.spaceRatio = this.widthSpace / this.heightSpace;

	}

	public void ZoomIn() {
		this.nodeHigh += NODE_HEIGHT_STEP;
		this.heightSpace += SPACE_HEIGHT_STEP;
		this.nodeWidth += this.nodeRatio * this.NODE_HEIGHT_STEP;
		this.widthSpace += this.spaceRatio * this.SPACE_HEIGHT_STEP;
		miniAdjust();
	}

	public void ZoomOut() {
		this.nodeHigh -= NODE_HEIGHT_STEP;
		this.heightSpace -= SPACE_HEIGHT_STEP;
		this.nodeWidth -= this.nodeRatio * this.NODE_HEIGHT_STEP;
		this.widthSpace -= this.spaceRatio * this.SPACE_HEIGHT_STEP;
		miniAdjust();
	}

	private void miniAdjust() {
		//add
		if (this.nodeHigh > this.MAX_NODE_HEIGHT) {
			this.nodeHigh = this.MAX_NODE_HEIGHT;
		}
		if (this.nodeWidth > this.MAX_NODE_HEIGHT * this.nodeRatio) {
			this.nodeWidth = (int) (this.MAX_NODE_HEIGHT * this.nodeRatio);
		}
		if (this.heightSpace > this.MAX_SPACE_HEIGHT) {
			this.heightSpace = this.MAX_SPACE_HEIGHT;
		}
		if (this.widthSpace > this.MAX_SPACE_HEIGHT * this.spaceRatio) {
			this.widthSpace = (int) (this.MAX_SPACE_HEIGHT * this.spaceRatio);
		}

		//sub
		if (this.nodeHigh < this.MIN_NODE_HEIGHT) {
			this.nodeHigh = this.MIN_NODE_HEIGHT;
		}

		if (this.nodeWidth < this.MIN_NODE_HEIGHT * this.nodeRatio) {
			this.nodeWidth = (int) (this.nodeRatio * this.MIN_NODE_HEIGHT);
		}

		if (this.heightSpace < this.MIN_SPACE_HEIGHT) {
			this.heightSpace = this.MIN_SPACE_HEIGHT;
		}

		if (this.widthSpace < this.MIN_SPACE_HEIGHT * this.spaceRatio) {
			this.widthSpace = (int) (this.MIN_SPACE_HEIGHT * this.spaceRatio);
		}

	}

	public void Zoom(double percent) {
		this.nodeHigh = (int) (percent * this.DEFAULT_NODE_HIGH);
		if (this.isValidate) {
			this.nodeWidth = (int) (percent * this.DEFAULT_NODE_WIDTH_WITHOUT_VALIDATION);
		} else {
			this.nodeWidth = (int) (percent * this.DEFAULT_NODE_WIDTH);
		}
		this.heightSpace = (int) (percent * this.DEFAULT_HEIGHT_SPACE);
		this.widthSpace = (int) (percent * this.DEFAULT_WIDTH_SPACE);
		miniAdjust();
	}

	public Font getFont() {
		Font f = null;
		int scale = this.nodeHigh / this.NODE_HEIGHT_STEP;
		//family=SansSerif,name=sansserif,style=plain,size=12
		if (scale >= FONTSIZESET.length) {
			f = new Font("sansserif", 0, FONTSIZESET[FONTSIZESET.length - 1]);
		} else {
			f = new Font("sansserif", 0, FONTSIZESET[scale]);
		}
		return f;
	}

	public Font geteEdgeFont() {
		Font f = null;
		int scale = this.nodeHigh / this.NODE_HEIGHT_STEP;
		//family=SansSerif,name=sansserif,style=plain,size=12
		if (scale >= EDGEFONTSIZESET.length) {
			f = new Font("sansserif", 0, EDGEFONTSIZESET[EDGEFONTSIZESET.length - 1]);
		} else {
			f = new Font("sansserif", 0, EDGEFONTSIZESET[scale]);
		}
		return f;
	}

	public int getEdgeMaxWidth() {
		int scale = this.nodeHigh / this.NODE_HEIGHT_STEP;
		int width = 1;
		if (scale >= EDGEMAXWIDTH.length) {
			width = EDGEMAXWIDTH[EDGEMAXWIDTH.length - 1];
		} else {
			width = EDGEMAXWIDTH[scale];
		}
		return width;
	}

}
