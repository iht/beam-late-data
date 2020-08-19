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

package com.google.cloud.pso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.cloud.pso.data.MyDummyEvent;
import com.google.cloud.pso.data.PaneGroupMetadata;
import com.google.cloud.pso.dofn.AggAndCountWindows;
import com.google.cloud.pso.windows.SampleSessionWindow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class LateDropOrNotTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();
  private int N_ONTIME = 50; // Number of on-time events
  private int N_LATE = 10; // Number of late events

  // All messages will have the same key. The values themselves are not important
  private String MSG_KEY = "my msg key";

  // The stream begins at this moment
  private Instant TEST_EPOCH = Instant.parse("2017-02-03T10:37:30.00Z");

  /**
   * Generate a list of on-time data
   *
   * @return A list of timestamped values that are on time.
   */
  private List<TimestampedValue<KV<String, MyDummyEvent>>> onTimeData() {
    return MyDummyEvent.generateData(N_ONTIME, MSG_KEY, false, TEST_EPOCH);
  }

  /**
   * Generate a list of late data.
   *
   * <p>These messages will be tagged with isLate to true, but they will be only late because they
   * are delivered much later than messages in the same window. So in order for these messages to be
   * "real" late data, they should be delivered after a while, compared to the initial messages.
   * They will have similar timestamps, but will arrive later than on-time messages.
   *
   * @return A list of timestamped values that are meant to be delivered late.
   */
  private List<TimestampedValue<KV<String, MyDummyEvent>>> lateData() {
    return MyDummyEvent.generateData(N_ONTIME, MSG_KEY, true, TEST_EPOCH);
  }

  /**
   * Estimate watermark with a heuristics similar to PubSub.
   *
   * <p>We need a backlog of events (maybe already acked) to estimate the watermark. Don't feed late
   * events to this method, as late events should not alter the watermark.
   *
   * @param events The backlog of all on time events to be processed.
   * @param alreadySeen The number of messages that have been already acked, in timestamp order.
   * @return An instant, which is an estimation of the watermark for this subscription.
   */
  private Instant estimateWatermark(
      List<TimestampedValue<KV<String, MyDummyEvent>>> events, Integer alreadySeen) {
    if (alreadySeen >= events.size()) alreadySeen = events.size() - 1;

    List<Instant> ts = new ArrayList<>();
    for (int k = 0; k < events.size(); k++) ts.add(events.get(k).getTimestamp());
    Collections.sort(ts);

    return ts.get(alreadySeen);
  }

  /** Test for the window strategies and how messages are grouped and counted. */
  @Test
  public void testWindowAndTriggers() {
    /** Prepare data */
    List<TimestampedValue<KV<String, MyDummyEvent>>> onTimeEvents = onTimeData();
    List<TimestampedValue<KV<String, MyDummyEvent>>> lateEvents = lateData();

    TestStream.Builder<KV<String, MyDummyEvent>> base =
        TestStream.create(KvCoder.of(AvroCoder.of(String.class), AvroCoder.of(MyDummyEvent.class)));

    // Add on-time events
    Instant currentWatermark = TEST_EPOCH;
    for (int k = 0; k < N_ONTIME; k++) {
      currentWatermark = estimateWatermark(onTimeEvents, k);
      base =
          base.addElements(onTimeEvents.get(k))
              .advanceProcessingTime(Duration.standardSeconds(2))
              .advanceWatermarkTo(currentWatermark);
    }

    currentWatermark = currentWatermark.plus(Duration.standardSeconds(2));

    // Wait some processing time
    base = base.advanceProcessingTime(Duration.standardSeconds(3));
    base.advanceWatermarkTo(currentWatermark);

    // Add late data
    // This data is late because their timestamps are older than the watermark
    // We increase the watermark with every late message by 2 seconds.
    for (int k = 0; k < N_LATE; k++) {
      currentWatermark = currentWatermark.plus(Duration.standardSeconds(2));
      base =
          base.addElements(lateEvents.get(k))
              .advanceProcessingTime((Duration.standardSeconds(2)))
              .advanceWatermarkTo(currentWatermark);
    }

    // Let's finalize the stream
    TestStream<KV<String, MyDummyEvent>> st =
        base.advanceProcessingTime((Duration.standardSeconds(2))).advanceWatermarkToInfinity();

    /** Streaming pipeline */
    PCollection<KV<String, MyDummyEvent>> events = pipeline.apply(st);
    PCollection<KV<String, MyDummyEvent>> windowed = events.apply(new SampleSessionWindow());
    // Identity, just to count elements
    PCollection<KV<String, MyDummyEvent>> identity =
        windowed.apply(
            "Identity transform (to count processed events)",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(String.class), TypeDescriptor.of(MyDummyEvent.class)))
                .via(
                    (KV<String, MyDummyEvent> kv) -> {
                      AggAndCountWindows.NUM_PROCESSED_EVENTS_BEFORE_WINDOW++;
                      return kv;
                    }));

    // We group, and then we will sum. We should get as many partial sums as triggers
    PCollection<KV<String, Iterable<MyDummyEvent>>> grouped =
        identity.apply("Group", GroupByKey.create());

    // Keep track of window id for each trigger, count number of different windows and number of
    // triggers
    PCollection<PaneGroupMetadata> summed =
        grouped.apply("Sum groups", ParDo.of(new AggAndCountWindows()));

    pipeline.run();

    /** Test assertions */
    List<TimestampedValue<KV<String, MyDummyEvent>>> allEvents = new ArrayList<>();
    allEvents.addAll(onTimeEvents);
    allEvents.addAll(lateEvents);

    // Make sure all messages are processed before windowing
    assertEquals(
        "All messages are processed before windowing",
        N_ONTIME + N_LATE,
        AggAndCountWindows.NUM_PROCESSED_EVENTS_BEFORE_WINDOW);

    // Make sure all messages are counted after windowing (no late messages are dropped)
    assertEquals(
        "No late messages are dropped after windowing",
        AggAndCountWindows.NUM_PROCESSED_EVENTS_BEFORE_WINDOW,
        AggAndCountWindows.NUM_PROCESSED_EVENTS_AFTER_WINDOW);
  }
}
