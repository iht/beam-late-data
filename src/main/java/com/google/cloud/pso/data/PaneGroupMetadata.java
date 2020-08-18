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
import lombok.Builder;
import lombok.NonNull;
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
@Builder
public class PaneGroupMetadata {

  @NonNull
  private int
      numberOfTriggers; // Number of triggers up to this point (to order the rows in the output)

  @NonNull private String key; // Key of this group
  @NonNull private String windowBoundaries; // Window boundaries (timestamps) as a string
  @NonNull private Boolean isFirstPane; // Is this the first pane/trigger of this window?
  @NonNull private Boolean isLastPane; // Is it the last one?
  @NonNull private String timing; // EARLY, ON_TIME or LATE
  @NonNull private Instant lastTimestamp; // Last timestamp seen in this pane
  @NonNull private int numEventsBeforeWindow; // Num. of events seen before applying the window

  @NonNull
  private int
      numEventsAfterWindow; // Num. of events seen after the window (same if no dropped data)

  @NonNull private int groupSize;   // Number of elements in this group

  public String toCSVLine() {
    String line = String.format(
            "%d,%s,%s,%b,%b,%s,%s,%d,%d,%d",
            numberOfTriggers,
            key,
            windowBoundaries,
            isFirstPane,
            isLastPane,
            isLastPane,
            lastTimestamp.toString(),
            numEventsBeforeWindow,
            numEventsAfterWindow,
            groupSize);

    return line;
  }
}
