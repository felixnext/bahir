#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 A sample wordcount with ZeroMQStream stream
 Usage: zeromq_wordcount.py <publisher url> <topic>

 How to run this example locally:

 (1) Run the publisher:

    `$ bin/run-example \
      org.apache.spark.examples.streaming.zeromq.SimpleZeroMQPublisher tcp://localhost:1234 foo`

 (2) Run the example:

    `$ bin/run-example \
      streaming-zeromq/examples/src/main/python/streaming/zeromq_wordcount.py tcp://localhost:1234 foo`
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from zeromq import ZeroMQUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: zeromq_wordcount.py <publisher url> <topic>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingZeroMQWordCount")
    ssc = StreamingContext(sc, 1)

    brokerUrl = sys.argv[1]
    topic = sys.argv[2]

    lines = ZeroMQUtils.createStream(ssc, brokerUrl, topic)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
