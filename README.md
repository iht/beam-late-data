# Exactly-Once Processing and Windowing in Streaming Pipelines (with Apache Beam)

This repository contains the code showcased in the Beam Summit 2020 talk:
**"Understanding Exactly-Once Processing and Windowing in Streaming Pipelines"**.

[![Slides](https://img.shields.io/badge/Slides-PDF-red)](docs/slides.pdf)
[![Talk](https://img.shields.io/badge/Talk-Beam%20Summit-blue)](https://2020.beamsummit.org/sessions/understanding-exactly-once-processing/)

This repository serves as a hands-on educational guide to understanding Apache Beam windowing behavior. It provides a concrete, runnable example to help you visualize:
- How different windowing strategies group data.
- How triggers control when results are materialized.
- How "allowed lateness" determines whether late-arriving data is processed or dropped.
- How watermarks progress and impact aggregation.

By running the tests and analyzing the output, you can build intuition for these critical streaming concepts.

---

## Watch the Talk

- Watch the video at the [Beam Summit website](https://2020.beamsummit.org/sessions/understanding-exactly-once-processing/).
- [Slides (PDF)](docs/slides.pdf).

---

## Core Concepts

To get the most out of this example, it helps to understand these core streaming concepts:

*   **Event Time vs. Processing Time**:
    *   **Event Time**: The time when the event originally occurred (e.g., device log timestamp).
    *   **Processing Time**: The time when the event is processed in your Beam pipeline.
    *   **Watermark**: Beam's estimation of event-time progress. A watermark of $T$ means the system assumes it has observed all data with event time $t < T$.
*   **Late Data**: Data is "late" if it arrives after the watermark has passed its event timestamp. When aggregating data in windows, you must configure how to handle late data (e.g., using `allowedLateness` to wait longer, or letting it drop).
*   **Deterministic Testing with `TestStream`**: Testing streaming pipelines with real-time clocks is unreliable. Beam's `TestStream` allows us to programmatically advance the watermark and processing time, making tests deterministic and reproducible. This is key to verifying how windows and triggers behave under different latency scenarios.

---

## The Tested Pipeline

The pipeline processes 60 messages: 50 messages produced on time, and 10 messages that arrive after the watermark (late data).

The test steps are:
1.  [We first add the 50 messages and advance the watermark](src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L117-L131).
2.  [Then we add 10 messages with a timestamp older than the watermark, advancing the watermark by 2 seconds per message](src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L133-L142).
3.  [We read the messages and count them before applying the window](src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L149-L161).
4.  [We apply the window, group, calculate a sum, and generate a CSV](src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L163-L178).
5.  [Finally, we assert if the window dropped any messages](src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L201-L211).

---

## How to Test Your Own Window

### First: Add your window

Add a new window to `src/main/java/com/google/cloud/pso/windows/SomeSampleWindow.java`.

Create a new method with this signature:
```java
public Window<KV<String, MyDummyEvent>> myCustomWindow()
```
See [examples of windows in that file](src/main/java/com/google/cloud/pso/windows/SomeSampleWindow.java#L64-L110).

You also need to:
1.  Add a new enum value to `WindowType` in `SomeSampleWindow.java`.
2.  Update the switch-case in the `SomeSampleWindow` constructor to map your new enum value to your window method.

### Second: Apply your window

To apply your window in the test:
1.  Open `src/test/java/com/google/cloud/pso/LateDropOrNotTest.java`.
2.  Locate where the window is applied (around line 163):
    ```java
    PCollection<KV<String, MyDummyEvent>> windowed =
        identity.apply(new SomeSampleWindow(WindowType.SESSION_WINDOW_AFTER_EACH_IN_ORDER));
    ```
3.  Change `WindowType.SESSION_WINDOW_AFTER_EACH_IN_ORDER` to your new enum value.

---

## Running Tests

You can run the tests using Maven:

```bash
mvn test
```

The tests will generate CSV outputs in `target/output/<timestamp>/`.

---

## Interpreting Results

You can inspect the generated CSV output using the helper script:

```bash
./scripts/show_data.sh target/output/<RUN_TIMESTAMP>
```

This will produce a table similar to this:

| triggers | window | is_first | is_last | timing | n_before | n_after | N |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | [10:36:40..10:38:20) | True | False | EARLY | 3 | 3 | 3 |
| ... | ... | ... | ... | ... | ... | ... | ... |
| 17 | [10:36:40..10:38:20) | False | False | ON_TIME | 50 | 49 | 49 |
| 18 | [10:36:40..10:38:20) | False | False | LATE | 51 | 50 | 50 |

### Columns Explanation
- **triggers**: The index of the trigger firing.
- **window**: The boundaries of the window for this pane.
- **is_first / is_last**: Indicates if this is the first or last pane fired for this window.
- **timing**: Indicates when the pane fired relative to the watermark (`EARLY` before watermark, `ON_TIME` at watermark, `LATE` after watermark).
- **n_before**: Number of elements processed by the pipeline *before* the window transform.
- **n_after**: Number of elements accumulated in the window *after* the window transform.
- **N**: Number of elements included in this specific pane.

If `n_after` at the final trigger is less than `n_before`, it indicates that some late data was dropped.

---

## Copyright

Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
