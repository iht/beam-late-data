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

package com.google.cloud.pso.windows;

import com.google.cloud.pso.data.MyDummyEvent;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SomeSampleWindow
    extends PTransform<
        PCollection<KV<String, MyDummyEvent>>, PCollection<KV<String, MyDummyEvent>>> {

  private Window<KV<String, MyDummyEvent>> windowToApply;

  public SomeSampleWindow(WindowType windowType) {
    switch (windowType) {
      case SESSION_WINDOW_WATERMARK:
        windowToApply = sessionWindowWatermark();
        break;
      case SESSION_WINDOW_AFTER_EACH_IN_ORDER:
        windowToApply = sessionWindowAfterEach();
        break;
      case FIXED_WINDOW_DROP:
        windowToApply = fixedWindowDropLateData();
        break;
      case FIXED_WINDOW_NO_DROP:
      default:
        windowToApply = fixedWindow();
    }
  }

  @Override
  public PCollection<KV<String, MyDummyEvent>> expand(PCollection<KV<String, MyDummyEvent>> input) {
    PCollection<KV<String, MyDummyEvent>> output = input.apply(windowToApply);

    return output;
  }

  /** A fixed window with no dropped late data */
  public Window<KV<String, MyDummyEvent>> fixedWindow() {
    Window<KV<String, MyDummyEvent>> window =
        Window.<KV<String, MyDummyEvent>>into(FixedWindows.of(Duration.standardSeconds(100)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(5)))
                    .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
            .withAllowedLateness(Duration.standardDays(10))
            .accumulatingFiredPanes();

    return window;
  }

  /** A fixed window dropping late data */
  public Window<KV<String, MyDummyEvent>> fixedWindowDropLateData() {
    Window<KV<String, MyDummyEvent>> window =
        Window.<KV<String, MyDummyEvent>>into(FixedWindows.of(Duration.standardSeconds(10)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(5)))
                    .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
            .withAllowedLateness(Duration.standardSeconds(0))
            .accumulatingFiredPanes();

    return window;
  }

  /** A session window with firing based on AfterEach.inOrder */
  public Window<KV<String, MyDummyEvent>> sessionWindowAfterEach() {
    Window<KV<String, MyDummyEvent>> window =
        Window.<KV<String, MyDummyEvent>>into(Sessions.withGapDuration(Duration.standardSeconds(5)))
            .triggering(
                AfterEach.inOrder(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(8)),
                    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane())))
            .withAllowedLateness(Duration.standardDays(10))
            .accumulatingFiredPanes();

    return window;
  }

  /** A session window with firing based on watermark */
  private Window<KV<String, MyDummyEvent>> sessionWindowWatermark() {
    Window<KV<String, MyDummyEvent>> window =
        Window.<KV<String, MyDummyEvent>>into(Sessions.withGapDuration(Duration.standardSeconds(5)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(8)))
                    .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
            .withAllowedLateness(Duration.standardDays(10))
            .accumulatingFiredPanes();

    return window;
  }

  public enum WindowType {
    FIXED_WINDOW_NO_DROP,
    FIXED_WINDOW_DROP,
    SESSION_WINDOW_AFTER_EACH_IN_ORDER,
    SESSION_WINDOW_WATERMARK
  }
}
