#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


if [[ "$#" -gt 0 ]]
then
  DOCKER_TAG=$1
else
  DOCKER_TAG="pinot:latest"
  echo "Not specified a Docker Tag, using default tag: ${DOCKER_TAG}."
fi

PINOT_VERSION=$2

ENV=$3

echo "Trying to build Pinot docker image and tag it as: [ ${DOCKER_TAG} ] ."

rm -rf ./agent.tar
rm -rf ./hadoop-2.7.0.tar
rm -rf ./conf.tar
tar -cvf conf.tar ams bts fra dfw sjc lab
tar -cvf agent.tar ./agent
tar -cvf hadoop-2.7.0.tar ./hadoop-2.7.0
docker build . -t registry-qa.webex.com/pinot/pinot:${DOCKER_TAG} --build-arg PINOT_VERSION=${PINOT_VERSION}