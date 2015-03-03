/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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
 * limitations under the License. */

package ezbake.data.common;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Stopwatch;

import ezbake.base.thrift.EzSecurityToken;

public abstract class LoggingUtils {
    private LoggingUtils() {}

    public static Stopwatch createStopWatch() {
        return Stopwatch.createStarted();
    }

    public static void stopAndLogStopWatch(Logger logger, Stopwatch watch, String name) {
        watch.stop();
        logger.info("{}|{} miliseconds", name, watch.elapsed(TimeUnit.MILLISECONDS));
    }

    public static void logResetAndStartStopWatch(Logger logger, Stopwatch watch, String name) {
        watch.stop();
        logger.info("{}|{} miliseconds", name, watch.elapsed(TimeUnit.MILLISECONDS));
        watch.reset();
        watch.start();
    }

    public static String secLog(EzSecurityToken security, Object message) {
        return extractUserInfo(security) + ", " + message.toString();
    }

    private static String extractUserInfo(EzSecurityToken security) {
        final String s = security.getTokenPrincipal().getName() + " , " + security.getAuthorizations();
        return s;
    }
}
