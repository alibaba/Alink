package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.clustering.HasEpsilon;
import com.alibaba.alink.params.shared.clustering.HasMinPoints;
import com.alibaba.alink.params.shared.colname.HasGroupCols;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface GroupGeoDbscanParams<T> extends
	HasIdCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasLatitudeCol <T>,
	HasLongitudeCol <T>,
	HasPredictionCol <T>,
	HasGroupCols <T>,
	HasMinPoints <T>,
	HasEpsilon <T>,
	HasGroupMaxSamples <T>,
	HasSkip <T> {
}
