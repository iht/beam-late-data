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

  private String windowBoundaries; // Window boundaries (timestamps) as a string
  private Boolean isFirstPane; // Is this the first pane/trigger of this window?
  private Boolean isLastPane; // Is it the last one?
  private String timing; // EARLY, ON_TIME or LATE
  private int numEventsBeforeWindow; // Num. of events seen before applying the window

  private int
      numEventsAfterWindow; // Num. of events seen after the window (same if no dropped data)

  private int groupSize; // Number of elements in this group

  // This constructor is necessary to run tests with JUnit
  public PaneGroupMetadata() {}

  public PaneGroupMetadata(
      int numberOfTriggers,
      String windowBoundaries,
      Boolean isFirstPane,
      Boolean isLastPane,
      String timing,
      int numEventsBeforeWindow,
      int numEventsAfterWindow,
      int groupSize) {
    this.numberOfTriggers = numberOfTriggers;
    this.windowBoundaries = windowBoundaries;
    this.isFirstPane = isFirstPane;
    this.isLastPane = isLastPane;
    this.timing = timing;
    this.numEventsBeforeWindow = numEventsBeforeWindow;
    this.numEventsAfterWindow = numEventsAfterWindow;
    this.groupSize = groupSize;
  }

  public String toCSVLine() {
    String line =
        String.format(
            "%d,%s,%b,%b,%s,%d,%d,%d",
            numberOfTriggers,
            windowBoundaries,
            isFirstPane,
            isLastPane,
            timing,
            numEventsBeforeWindow,
            numEventsAfterWindow,
            groupSize);

    return line;
  }
}
