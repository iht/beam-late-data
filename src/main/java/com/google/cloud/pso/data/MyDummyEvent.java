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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import lombok.Data;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Just a class to hold together all the fields related to an event */
@DefaultCoder(AvroCoder.class)
@Data
public class MyDummyEvent implements Comparable<MyDummyEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(MyDummyEvent.class);

  private static int last_msg_id = 0;
  private int msg_id;

  private String key;
  private int type;
  private long eventTimestamp;
  private boolean isLate;

  public MyDummyEvent() {}

  public MyDummyEvent(String key, int type, long eventTimestamp, boolean isLate) {
    this.key = key;
    this.type = type;
    this.eventTimestamp = eventTimestamp;
    this.isLate = isLate;

    last_msg_id++;
    this.msg_id = last_msg_id;
  }

  public static MyDummyEvent createRandomEvent(String key, Long timestamp, boolean isLate) {
    Random r = new Random();
    int type = r.ints(0, 10).findFirst().getAsInt();

    return new MyDummyEvent(key, type, timestamp, isLate);
  }

  /**
   * Generate a list of messages
   *
   * @param N Number of messages to generate
   * @param msgKey The key for the messages to be generated
   * @param isLate Whether we should tag or not these messages as late. This tag is only used for
   *     reporting.
   * @param testEpoch The oldest possible timestamp for the values.
   * @return A list of timestampted values, starting from the testEpoch
   */
  public static List<TimestampedValue<KV<String, MyDummyEvent>>> generateData(
      int N, String msgKey, boolean isLate, org.joda.time.Instant testEpoch) {
    Random r = new Random();
    List<TimestampedValue<KV<String, MyDummyEvent>>> events = new ArrayList<>();

    for (int k = 0; k < N; k++) {
      // Generate events shifted ~1 sec from each other
      Long shift = r.longs(100, 2999).findFirst().getAsLong();
      Long ts = testEpoch.plus(shift).plus(1000 * (k + 1)).getMillis();

      MyDummyEvent event = MyDummyEvent.createRandomEvent(msgKey, ts, isLate);

      KV<String, MyDummyEvent> val = KV.of(event.getKey(), event);
      TimestampedValue<KV<String, MyDummyEvent>> tsval =
          TimestampedValue.of(val, new org.joda.time.Instant(ts));

      events.add(tsval);
    }

    return events;
  }

  public LocalDateTime getDateTime() {
    return LocalDateTime.ofInstant(
        Instant.ofEpochSecond(this.eventTimestamp), TimeZone.getDefault().toZoneId());
  }

  /** This must be overridden to be able to sort collections of `MyDummyEvent` */
  @Override
  public int compareTo(MyDummyEvent e) {

    // If we are comparing against null, we return this as the lower value
    if (e == null) return 1;

    Long compare = e.getEventTimestamp();

    // Just return 0 if it is exactly the same timestamp
    if (compare == this.eventTimestamp) return 0;

    Long diff = Math.subtractExact(this.eventTimestamp, compare);

    // We convert to int, because that's what compareTo expects
    // If we overflow, we set to the maximum value possible
    int comparison = 0;

    try {
      comparison = Math.toIntExact(diff);
    } catch (ArithmeticException arithException) {
      if (diff > 0L) comparison = Integer.MAX_VALUE;
      else comparison = Integer.MIN_VALUE;
    }

    LOG.info(
        String.format(
            "  XXXX COMPARISON:  %d-%d = %d  converted to int as %d",
            this.eventTimestamp, compare, diff, comparison));

    return comparison;
  }
}
