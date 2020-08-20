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

package com.google.cloud.pso.dofn;

import com.google.cloud.pso.data.MyDummyEvent;
import com.google.cloud.pso.data.PaneGroupMetadata;
import com.google.common.collect.Iterables;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;

/**
 * This class produces a set of counts of the number of events and triggers seen after applying a
 * window.
 *
 * <p>It is used during the unit tests, to evaluate the impact of different windowing strategies.
 */
public class AggAndCountWindows
    extends DoFn<KV<String, Iterable<MyDummyEvent>>, PaneGroupMetadata> {

  public static int NUM_TRIGGERS = 0;
  public static Set<String> SEEN_WINDOWS_IN_TRIGGER = new HashSet<>();
  // public static Set<MyDummyEvent> SEEN_EVENTS_AFTER_WINDOW = new HashSet<>();
  public static int NUM_PROCESSED_EVENTS_BEFORE_WINDOW = 0;
  //public static int NUM_PROCESSED_EVENTS_AFTER_WINDOW = 0;

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow w) {
    // The input is the list of dummy events, grouped together by key, after applying the window
    KV<String, Iterable<MyDummyEvent>> kv = c.element();
    // This DoFn is applied after a GroupByKey and a Window, so this will run once per trigger
    NUM_TRIGGERS++;

    // Events grouped after GroupByKey
    Iterable<MyDummyEvent> vals = kv.getValue();
    // Number of events in group
    int size = Iterables.size(vals);
    List<Long> timestamps = new ArrayList<>();
    for (MyDummyEvent event : vals) {
      timestamps.add(event.getEventTimestamp());
      // This must be a set because the same event will be visited once per trigger
      SEEN_WINDOWS_IN_TRIGGER.add(event.toString());
    }

    // Order by timestamp
    Collections.sort(timestamps);

    PaneGroupMetadata paneGroupMetadata =
        new PaneGroupMetadata(
            NUM_TRIGGERS,
            w.toString(),
            c.pane().isFirst(),
            c.pane().isLast(),
            c.pane().getTiming().toString(),
            NUM_PROCESSED_EVENTS_BEFORE_WINDOW,
            SEEN_WINDOWS_IN_TRIGGER.size(),
            size);

    c.output(paneGroupMetadata);
  }
}
