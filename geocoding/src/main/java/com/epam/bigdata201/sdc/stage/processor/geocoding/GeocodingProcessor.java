/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.epam.bigdata201.sdc.stage.processor.geocoding;

import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class GeocodingProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GeocodingProcessor.class);

  // Gives access to the UI configuration of the stage provided
  // by the {@link GeocodingDProcessor} class.
  public abstract String getApiKey();

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return super.init();
  }

  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  private JOpenCageLatLng getCoordinates(String address) {
    JOpenCageForwardRequest request = new JOpenCageForwardRequest(address);

    JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder(getApiKey());
    JOpenCageResponse response = jOpenCageGeocoder.forward(request);

    LOG.info("Coordinates for \"{}\": {}, {}", address,
            response.getFirstPosition().getLat().toString(),
            response.getFirstPosition().getLng().toString());
    return response.getFirstPosition();
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // processes input record and writes it to the output stream
    JOpenCageLatLng coords = getCoordinates(record.get("/Address").getValueAsString());
    record.set("/Latitude", Field.create(coords.getLat().toString()));
    record.set("/Longitude", Field.create(coords.getLng().toString()));

    batchMaker.addRecord(record);
  }

}