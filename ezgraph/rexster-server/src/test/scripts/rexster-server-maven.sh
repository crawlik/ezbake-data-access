#!/bin/sh
#   Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

if [ -z "$EZCONFIGURATION_DIR" ]; then
    export EZCONFIGURATION_DIR=src/test/scripts
fi

# Run EzBake Rexster server from top-level of EzBake Rexster subproject.

if [ ! -d config ]; then
    echo "Missing 'config' directory."
    echo "Is this run from the top level of the EzBake Rexster subproject?"
    exit 1
fi

# If the server artifact doesn't exist, attempt to build it. This will not
# attempt to rebuild after changes - you should be calling `mvn package`
# manually.
artifact="target/ezbake-rexster-server-2.1-SNAPSHOT.jar"
if [ ! -f $artifact ]; then
    mvn package
fi

# Form the classpath out of all of the dependencies
classpath=`find target/dependency -name "*.jar" | tr '\n' ':'`

# classpath var now has a trailing ':', append the project's artifact to it
classpath="$classpath$artifact"

java -cp $classpath ezbake.data.graph.rexster.StandaloneApplication
