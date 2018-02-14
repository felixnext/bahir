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

#from py4j.protocol import Py4JJavaError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import UTF8Deserializer
from pyspark.streaming import DStream

__all__ = ['ZeroMQUtils']


class ZeroMQUtils(object):

    @staticmethod
    def createStream(ssc, publisherUrl, topic,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        Create an input stream that pulls messages from a ZeroMQ Broker.

        :param ssc:  StreamingContext object
        :param brokerUrl:  Url of remote zeromq publisher
        :param topic:  topic name to subscribe to
        :param storageLevel:  RDD storage level.
        :return: A DStream object
        """
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        helper = ZeroMQUtils._get_helper(ssc._sc)
        jstream = helper.createStream(ssc._jssc, publisherUrl, topic, jlevel)
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def _get_helper(sc):
        try:
            return sc._jvm.org.apache.spark.streaming.zeromq.ZeroMQUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                ZeroMQUtils._printErrorMsg(sc)
            raise

    @staticmethod
    def _printErrorMsg(sc):
        scalaVersionString = sc._jvm.scala.util.Properties.versionString()
        import re
        scalaVersion = re.sub(r'version (\d+\.\d+)\.\d+', r'\1', scalaVersionString)
        sparkVersion = re.sub(r'(\d+\.\d+\.\d+).*', r'\1', sc.version)
        print("""
________________________________________________________________________________________________

  Spark Streaming's ZeroMQ libraries not found in class path. Try one of the following.

  1. Include the MQTT library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.bahir:spark-streaming-zeromq_%s:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-zeromq-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-zeromq-assembly.jar> ...
________________________________________________________________________________________________
""" % (scalaVersion, sparkVersion, sparkVersion))
