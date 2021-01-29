package com.epam.bigdata201.sdc.stage.processor.geohash;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class GeohashLengthChooserValues extends BaseEnumChooserValues<GeohashLengthValues> {
    public GeohashLengthChooserValues() {
        super(GeohashLengthValues.values());
    }
}
