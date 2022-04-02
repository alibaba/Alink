package com.alibaba.alink.params.image;

import com.alibaba.alink.params.io.HasRootFilePath;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasTensorCol;

public interface WriteTensorToImageParams<T>
	extends HasRootFilePath <T>,
	HasTensorCol <T>,
	HasRelativeFilePathCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasImageType <T> {
}
