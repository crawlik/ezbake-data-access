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

import java.text.SimpleDateFormat;
import java.util.Calendar;

import ezbake.base.thrift.Date;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.Time;
import ezbake.base.thrift.TimeZone;


public class TimeUtil {
    /**
     * Converts milliseconds since the epoch into a thrift DateTime object
     * 
     * @param time ms since epoch
     * @return
     */
    public static DateTime convertToThriftDateTime(long time) {
        return convert(time);
    }

    /**
     * Returns the current time using the Thrift common types DateTime structure with TimeZone filled in.
     */
    public static DateTime getCurrentThriftDateTime() {
        return convert(null);
    }

    /**
     * Converts Thrift DateTime object back to a long representing the time in millis from epoch. NOTE: if the time is
     * not set on the object, the return milliseconds will correspond to the UTC time zone. If you want a Date only
     * converted to milliseconds in your timezone, the Time object must be set, along with the proper time zone
     * parameters.
     * 
     * @param date
     * @return
     */
    public static long convertFromThriftDateTime(DateTime date) {
        final Calendar calendar = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
        calendar.set(date.getDate().getYear(), date.getDate().getMonth() - 1, date.getDate().getDay(), 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        if (date.getTime() != null) {
            calendar.add(Calendar.HOUR_OF_DAY, date.getTime().getHour());
            calendar.add(Calendar.MINUTE, date.getTime().getMinute());
            if (date.getTime().isSetSecond()) {
                calendar.add(Calendar.SECOND, date.getTime().getSecond());
            }
            if (date.getTime().isSetMillisecond()) {
                calendar.add(Calendar.MILLISECOND, date.getTime().getMillisecond());
            }
            if (date.getTime().getTz().isAfterUTC()) {
                calendar.add(Calendar.HOUR_OF_DAY, 0 - date.getTime().getTz().getHour());
                calendar.add(Calendar.MINUTE, 0 - date.getTime().getTz().getMinute());
            } else {
                calendar.add(Calendar.HOUR_OF_DAY, date.getTime().getTz().getHour());
                calendar.add(Calendar.MINUTE, date.getTime().getTz().getMinute());
            }
        }
        return calendar.getTime().getTime();
    }

    private static DateTime convert(Long time) {
        final SimpleDateFormat sd = new SimpleDateFormat("dd,HH");

        final java.util.TimeZone tz = java.util.TimeZone.getTimeZone("UTC");
        final Calendar utcTime = Calendar.getInstance(tz);
        if (time != null) {
            utcTime.setTimeInMillis(time);
        }
        sd.setCalendar(utcTime);
        final String[] utcstr = sd.format(utcTime.getTime()).split(",");

        final Calendar localTime = Calendar.getInstance();
        if (time != null) {
            localTime.setTimeInMillis(time);
        }
        sd.setCalendar(localTime);
        final String[] localstr = sd.format(localTime.getTime()).split(",");

        final int utcDay = Integer.parseInt(utcstr[0]);
        final int localDay = Integer.parseInt(localstr[0]);

        final int utcHour = Integer.parseInt(utcstr[1]);
        final int localHour = Integer.parseInt(localstr[1]);

        int hourDiff = 0;
        boolean afterUtc = false;
        if (localDay > utcDay) {
            hourDiff = localHour + 24 - utcHour;
            afterUtc = true;
        } else if (localDay < utcDay) {
            hourDiff = utcHour + 24 - localHour;
        } else {
            hourDiff = Math.abs(utcHour - localHour);
        }

        final TimeZone bakeTz = new TimeZone();
        bakeTz.setHour((short) hourDiff);
        bakeTz.setMinute((short) 0);
        bakeTz.setAfterUTC(afterUtc);

        final Date d = new Date();
        d.setDay((short) localTime.get(Calendar.DAY_OF_MONTH));
        d.setMonth((short) (localTime.get(Calendar.MONTH) + 1));
        d.setYear((short) localTime.get(Calendar.YEAR));

        final Time t = new Time();
        t.setHour((short) localTime.get(Calendar.HOUR_OF_DAY));
        t.setMinute((short) localTime.get(Calendar.MINUTE));
        t.setSecond((short) localTime.get(Calendar.SECOND));
        t.setMillisecond((short) localTime.get(Calendar.MILLISECOND));
        t.setTz(bakeTz);

        final DateTime dt = new DateTime();
        dt.setDate(d);
        dt.setTime(t);

        return dt;
    }

    public static void main(String[] args) {

        System.err.println(getCurrentThriftDateTime());
    }
}
