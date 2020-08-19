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
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SampleSessionWindow
    extends PTransform<
        PCollection<KV<String, MyDummyEvent>>, PCollection<KV<String, MyDummyEvent>>> {

  @Override
  public PCollection<KV<String, MyDummyEvent>> expand(PCollection<KV<String, MyDummyEvent>> input) {
    PCollection<KV<String, MyDummyEvent>> output =

        // Window using AfterEach.inOrder
        //        input.apply(
        //            Window.<KV<String, MyDummyEvent>>into(
        //                    Sessions.withGapDuration(Duration.standardSeconds(3)))
        //                .triggering(
        //                    AfterEach.inOrder(
        //                        AfterProcessingTime.pastFirstElementInPane()
        //                            .plusDelayOf(Duration.standardSeconds(8)),
        //                        Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane())))
        //                .withAllowedLateness(Duration.standardDays(10))
        //                .accumulatingFiredPanes());

        // Window using AfterWatermark
        input.apply(
            Window.<KV<String, MyDummyEvent>>into(
                    Sessions.withGapDuration(Duration.standardSeconds(3)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(8)))
                        .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
                .withAllowedLateness(Duration.standardDays(10))
                .accumulatingFiredPanes());

    return output;
  }
}
