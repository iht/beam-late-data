# Understanding Exactly-Once Processing And Windowing In Streaming Pipelines (with Apache Beam)

This repo contains the code showcased in the Beam Summit talk 
_Understanding Exactly-Once Processing And Windowing In Streaming Pipelines_

In that talk, I explained how windowing works in streaming pipelines, and what are the decisions you have
to make in order to do complex event processing in streaming with Apache Beam.

Whenever we apply a window, there are always doubts about whether the window will drop data, or how many 
times (and when) will be the output triggered.

This repo contains a sample pipeline that uses unit testing to check if your window would drop data, 
and how many times would the window be trigered. You write your window in a function, and then use the unit
test to check the output of that pipeline. If the window drops data, the test will fail. In addition, you 
get a CSV output that you can examine to see how and when your window produced output.

# Watch the talk

Watch the video at the Beam Summit website, or at YouTube:
* https://2020.beamsummit.org/sessions/understanding-exactly-once-processing/

[Check also the slides and the notes of each slide](https://drive.google.com/file/d/1XOZ5EMSjVv1WwJe_X7kqhqSVm502pzfs/view?usp=sharing).

# The tested pipeline

The pipeline processes 60 messages. 50 messages produced on time, and 10 messages that arrive after the
watermark (late data).

* [We first add the 50 messages and advance the watermark](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L117-L131)
* [Then we add 10 messages with a timestamp older than the watermark, and we advance the watermark some seconds per message (to simulate some elapsed time)](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L133-L142)
* [We read the messages, and count them before applying the window](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L149-L161)
* [Then we apply the window, group, calculate a sum (and update another metric to count the aggregated messages), and generate a CSV](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L163-L178)
* [We can now check if the window dropped any message or not](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/test/java/com/google/cloud/pso/LateDropOrNotTest.java#L201-L211)

# How to test your own window?


## First: add your window

Add a new window to `src/main/java/com/google/cloud/pso/windows/SomeSampleWindow.java`.

For that, just add a new method with this signature:

`public Window<KV<String, MyDummyEvent>> myCustomWindow()`

(maybe with some input parameters if you want to use those in your window).

See [some examples of windows in that file](https://github.com/iht/beam-late-data/blob/df3b504e9c36e69dd60c22d66d9ff0efc9a849f3/src/main/java/com/google/cloud/pso/windows/SomeSampleWindow.java#L64-L110)

## Second: apply your window

TODO

# Copyright

Copyright 2020 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
