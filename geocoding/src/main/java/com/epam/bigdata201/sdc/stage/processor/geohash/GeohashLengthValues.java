package com.epam.bigdata201.sdc.stage.processor.geohash;

import com.streamsets.pipeline.api.Label;

public enum GeohashLengthValues implements Label {
    ONE("1"),
    TWO("2"),
    THREE("3"),
    FOUR("4"),
    FIVE("5"),
    SIX("6"),
    SEVEN("7"),
    EIGHT("8");

    private String name;

    GeohashLengthValues(String name) {
        this.name = name;
    }

    @Override
    public String getLabel() {
        return name;
    }
}
