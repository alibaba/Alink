package com.alibaba.alink.common.lazy;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;

import java.io.ByteArrayOutputStream;

/**
 * Base class for models who need to output PMML.
 */
public interface BasePMMLModelInfo {

	default String getPMML() {
		try {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			JAXBUtil.marshalPMML(toPMML(), new StreamResult(stream));
			return stream.toString();
		} catch (JAXBException e) {
			throw new RuntimeException("PMML write stream error!");
		}

	}

	PMML toPMML();
}
