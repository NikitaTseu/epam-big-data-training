package com.epam.bigdata201.kstreams.utils;

/**
 *The Timestampable interface allows to create a custom implementation of timestamp extraction
* */
public interface Timestampable {
    long getTimestamp();
}
