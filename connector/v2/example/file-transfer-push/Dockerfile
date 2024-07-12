#  Copyright 2023-2024 Dataport AöR
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# -buster is required to have apt available
FROM openjdk:11-jre-slim-buster

# by default curl is not available, so install it
RUN apt update && apt install curl -y

WORKDIR /app


# Copy config
COPY ./resources /resources
# Copy jar
COPY ./build/libs/dataspace-connector.jar /app

ENTRYPOINT java -jar dataspace-connector.jar
