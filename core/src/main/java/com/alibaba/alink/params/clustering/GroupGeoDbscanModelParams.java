package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasEpsilon;
import com.alibaba.alink.params.shared.clustering.HasMinPoints;
import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;

public interface GroupGeoDbscanModelParams<T> extends
	HasLatitudeCol <T>,
	HasLongitudeCol <T>,
	HasGroupColsDefaultAsNull <T>,
	HasMinPoints <T>,
	HasEpsilon <T>,
	HasGroupMaxSamples <T>,
	HasSkip <T> {

}
