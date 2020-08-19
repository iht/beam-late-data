/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.cloud.pso.data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * We are grouping events after applying a window.
 *
 * <p>The triggers specified in the window will produce each a pane per group of events (those
 * sharing the same key). This class holds together the metadata about a pane and group, so it can
 * be examined easily in the output.
 */
@DefaultCoder(AvroCoder.class)
public class PaneGroupMetadata {

  private int
      numberOfTriggers; // Number of triggers up to this point (to order the rows in the output)

  private String key; // Key of this group
  private String windowBoundaries; // Window boundaries (timestamps) as a string
  private Boolean isFirstPane; // Is this the first pane/trigger of this window?
  private Boolean isLastPane; // Is it the last one?
  private String timing; // EARLY, ON_TIME or LATE
  private long lastTimestamp; // Last timestamp seen in this pane
  private int numEventsBeforeWindow; // Num. of events seen before applying the window

  private int
      numEventsAfterWindow; // Num. of events seen after the window (same if no dropped data)

  private int groupSize; // Number of elements in this group

  // This constructor is necessary to run tests with JUnit
  public PaneGroupMetadata() {}

  public PaneGroupMetadata(
      int numberOfTriggers,
      String key,
      String windowBoundaries,
      Boolean isFirstPane,
      Boolean isLastPane,
      String timing,
      long lastTimestamp,
      int numEventsBeforeWindow,
      int numEventsAfterWindow,
      int groupSize) {
    this.numberOfTriggers = numberOfTriggers;
    this.key = key;
    this.windowBoundaries = windowBoundaries;
    this.isFirstPane = isFirstPane;
    this.isLastPane = isLastPane;
    this.timing = timing;
    this.lastTimestamp = lastTimestamp;
    this.numEventsBeforeWindow = numEventsBeforeWindow;
    this.numEventsAfterWindow = numEventsAfterWindow;
    this.groupSize = groupSize;
  }

  public String toCSVLine() {
    String line =
        String.format(
            "%d,%s,%s,%b,%b,%s,%s,%d,%d,%d",
            numberOfTriggers,
            key,
            windowBoundaries,
            isFirstPane,
            isLastPane,
            isLastPane,
            lastTimestamp,
            numEventsBeforeWindow,
            numEventsAfterWindow,
            groupSize);

    return line;
  }

  public LocalDateTime getDateTime() {
    return LocalDateTime.ofInstant(
        Instant.ofEpochSecond(this.lastTimestamp), TimeZone.getDefault().toZoneId());
  }
}
