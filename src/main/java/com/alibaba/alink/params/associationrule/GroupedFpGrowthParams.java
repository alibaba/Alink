package com.alibaba.alink.params.associationrule;

import com.alibaba.alink.params.shared.associationrules.*;
import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;

public interface GroupedFpGrowthParams<T> extends
    HasItemsCol<T>, HasGroupColDefaultAsNull<T>, HasMinSupportCountDefaultAsNeg1<T>, HasMinSupportPercentDefaultAs002<T>,
    HasMinConfidenceDefaultAs005<T>, HasMaxPatternLengthDefaultAs10<T>, HasMaxConsequentLengthDefaultAs1<T>,
    HasMinLiftDefaultAs1<T> {
}
