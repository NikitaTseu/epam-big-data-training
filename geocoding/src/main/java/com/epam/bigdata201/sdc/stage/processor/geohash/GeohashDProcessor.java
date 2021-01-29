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
package com.epam.bigdata201.sdc.stage.processor.geohash;

import com.streamsets.pipeline.api.*;

@StageDef(
    version = 1,
    label = "Geohash Processor",
    description = "Calculates geohash by latitude and longitude",
    icon = "default.png",
    onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class GeohashDProcessor extends GeohashProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "4",
      label = "Geohash length",
      displayPosition = 1,
      group = "GEOHASH"
  )
  @ValueChooserModel(GeohashLengthChooserValues.class)
  public String geohashLength;

  @Override
  public String getGeohashLength() {
    return geohashLength;
  }
}