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
import java.util.Random;
import java.util.TimeZone;
import lombok.Data;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Just a class to hold together all the fields related to an event */
@DefaultCoder(AvroCoder.class)
@Data
public class MyDummyEvent implements Comparable<MyDummyEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(MyDummyEvent.class);

  private static int last_msg_id = 0;
  private int msg_id;

  private int type;
  private long eventTimestamp;
  private int sessionID;
  private int firewallSerial;
  private boolean isLate;

  public MyDummyEvent(
      int type, long eventTimestamp, int sessionID, int firewallSerial, boolean isLate) {
    this.type = type;
    this.eventTimestamp = eventTimestamp;
    this.sessionID = sessionID;
    this.firewallSerial = firewallSerial;
    this.isLate = isLate;

    last_msg_id++;
    this.msg_id = last_msg_id;
  }

  public static MyDummyEvent createRandomEvent(
      int sessionID, int firewallSerial, Long timestamp, boolean isLate) {
    Random r = new Random();
    int type = r.ints(0, 10).findFirst().getAsInt();

    return new MyDummyEvent(type, timestamp, sessionID, firewallSerial, isLate);
  }

  public LocalDateTime getDateTime() {
    return LocalDateTime.ofInstant(
        Instant.ofEpochSecond(this.eventTimestamp), TimeZone.getDefault().toZoneId());
  }

  /**
   * Get the key for this event, in CSV format.
   *
   * @return String with a CSV-friendly representation of the key for the event.
   */
  public String getKey() {
    return String.format("%d,%d", sessionID, firewallSerial);
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
