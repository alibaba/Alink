package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

public class BaseTFTableModelData implements Serializable {

	protected Params meta;
	protected String[] tfInputCols;
	protected List <Row> tfModelRows;
	protected String tfOutputSignatureDef;
	protected TypeInformation <?> tfOutputSignatureType;

	// When loading model data, especially in stream predict operators, tfModelRows cost a lot of memory and sometimes
	// lead to OOM. There model data are written to file directly to avoid memory occupation.
	protected String tfModelZipPath;

	public BaseTFTableModelData() {
	}

	public BaseTFTableModelData(Params meta, String[] tfInputCols, List <Row> tfModelRows, String tfOutputSignatureDef, TypeInformation<?> tfOutputSignatureType) {
		this.meta = meta;
		this.tfInputCols = tfInputCols;
		this.tfModelRows = tfModelRows;
		this.tfOutputSignatureDef = tfOutputSignatureDef;
		this.tfOutputSignatureType = tfOutputSignatureType;
	}

	public Params getMeta() {
		return meta;
	}

	public BaseTFTableModelData setMeta(Params meta) {
		this.meta = meta;
		return this;
	}

	public String[] getTfInputCols() {
		return tfInputCols;
	}

	public BaseTFTableModelData setTfInputCols(String[] tfInputCols) {
		this.tfInputCols = tfInputCols;
		return this;
	}

	public List <Row> getTfModelRows() {
		return tfModelRows;
	}

	public BaseTFTableModelData setTfModelRows(List <Row> tfModelRows) {
		this.tfModelRows = tfModelRows;
		return this;
	}

	public String getTfModelZipPath() {
		return tfModelZipPath;
	}

	public BaseTFTableModelData setTfModelZipPath(String tfModelZipPath) {
		this.tfModelZipPath = tfModelZipPath;
		return this;
	}

	public String getTfOutputSignatureDef() {
		return tfOutputSignatureDef;
	}

	public BaseTFTableModelData setTfOutputSignatureDef(String tfOutputSignatureDef) {
		this.tfOutputSignatureDef = tfOutputSignatureDef;
		return this;
	}

	public TypeInformation <?> getTfOutputSignatureType() {
		return tfOutputSignatureType;
	}

	public BaseTFTableModelData setTfOutputSignatureType(TypeInformation <?> tfOutputSignatureType) {
		this.tfOutputSignatureType = tfOutputSignatureType;
		return this;
	}
}
