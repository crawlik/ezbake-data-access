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

import java.util.Calendar;
import java.util.GregorianCalendar;

import ezbake.base.thrift.Date;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.Time;
import ezbake.base.thrift.TimeZone;

public class DateTimeConverter {

    /**
     * Returns an offset String like -05:00, use Integer.valueOf(retVal) for a numeric.
     * 
     * @param date
     * @param tz
     * @return
     */
    public static String getOffsetHoursAndMinutes(java.util.Date date, java.util.TimeZone tz) {
        final int offsetInMillis = tz.getOffset(date.getTime());
        final String offset =
                String.format("%02d:%02d", Math.abs(offsetInMillis / 3600000), Math.abs(offsetInMillis / 60000 % 60));
        return (offsetInMillis >= 0 ? "+" : "-") + offset;
    }

    /**
     * Returns an offset String like -05, use Integer.valueOf(retVal) for a numeric.
     * 
     * @param date
     * @param tz
     * @return
     */
    public static String getOffsetHours(java.util.Date date, java.util.TimeZone tz) {
        final int offsetInMillis = tz.getOffset(date.getTime());
        final String offset = String.format("%02d", Math.abs(offsetInMillis / 3600000));
        return (offsetInMillis >= 0 ? "+" : "-") + offset;
    }

    public static String getOffsetHours() {
        final java.util.TimeZone tz = java.util.TimeZone.getDefault();
        final java.util.Date date = new java.util.Date();
        return getOffsetHours(date, tz);
    }

    // Used in TemporalDataSetHandler & TemporalDataSetHandlerTest
    public static Calendar transformDateTime(DateTime dt) {
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        java.util.TimeZone utz = null;

        if (dt != null) {
            final Date d = dt.getDate();
            if (d != null) {
                year = d.getYear();
                month = d.getMonth() - 1;
                day = d.getDay();
            }

            final Time t = dt.getTime();
            TimeZone tz = null;
            if (t != null) {
                hour = t.getHour();
                minute = t.getMinute();
                second = t.getSecond();
                tz = t.getTz();
            }

            if (tz != null) {
                if (tz.isSetHour()) {
                    if (tz.isAfterUTC()) {
                        utz = java.util.TimeZone.getTimeZone("GMT+" + tz.getHour() + ":00");
                    } else {
                        utz = java.util.TimeZone.getTimeZone("GMT-" + tz.getHour() + ":00");
                    }
                }
            }
        }
        final GregorianCalendar c = new GregorianCalendar(year, month, day, hour, minute, second);
        if (utz != null) {
            c.setTimeZone(utz);
        }

        return c;
    }

    /**
     * Create a java.util.Calendar instance given the specific date, time & hours offset.
     * 
     * Note: This method is used in the DataAccessTest and DateTimeConverterTest classes, we may want to consider
     * moving this method to a test package to limit it's runtime availability.
     * 
     * @param year the value used to set the YEAR calendar field in the calendar.
     * @param oneIndexedMonth A one-indexed (as opposed to zero-indexed) month
     * @param dayOfMonth the value used to set the DAY_OF_MONTH calendar field in the calendar
     * @param hourOfDay the value used to set the HOUR_OF_DAY calendar field in the calendar.
     * @param minute the value used to set the MINUTE calendar field in the calendar.
     * @param second the value used to set the SECOND calendar field in the calendar
     * @param offset An offset (in hours) used to construct a TimeZone instance (e.g. "GMT" + offset + ":00")
     * @return
     */
    public static Calendar toCalendar(int year, int oneIndexedMonth, int dayOfMonth, int hourOfDay, int minute,
            int second, int offset) {
        final GregorianCalendar c =
                new GregorianCalendar(year, oneIndexedMonth - 1, dayOfMonth, hourOfDay, minute, second);
        if (offset != 0) {
            c.setTimeZone(java.util.TimeZone.getTimeZone("GMT" + offset + ":00"));
        }
        return c;
    }


    /**
     * Convert date & time integer representation to a Thrift DateTime object. Note: This method is used by the
     * TemporalDataSetHandlerTest and should be considered carefully. However, since it does not require the caller to
     * provide a timezone, it probably shouldn't be used.
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param minute
     * @param second
     * @return An ezbake DateTime instance.
     */
    public static DateTime toDateTimeWithoutOffset(int year, int month, int day, int hour, int minute, int second) {
        final DateTime dt = new DateTime();
        final Date date = new Date();
        date.setMonth((short) month).setDay((short) day).setYear((short) year);
        dt.setDate(date);
        final Time t = new Time();
        final TimeZone tz = new TimeZone();
        t.setHour((short) hour).setMinute((short) minute).setSecond((short) second).setTz(tz);
        dt.setTime(t);
        return dt;
    }

    // Used in TemporalServlet
    public static DateTime toDateTimeWithoutOffset(java.util.Date gmtDate) {
        final Calendar cal = new GregorianCalendar();
        cal.setTime(gmtDate);

        final int intYear = cal.get(Calendar.YEAR);
        final int monthInt = cal.get(Calendar.MONTH) + 1;
        final int intDay = cal.get(Calendar.DAY_OF_MONTH);
        final int intHour = cal.get(Calendar.HOUR_OF_DAY);
        final int intMins = cal.get(Calendar.MINUTE);
        final int intSecond = cal.get(Calendar.SECOND);

        return toDateTimeWithoutOffset(intYear, monthInt, intDay, intHour, intMins, intSecond);
    }

    /**
     * This method does not require the caller to provide a timezone, it probably shouldn't be used.
     * 
     * Note: This is used in TestThriftClient
     * 
     * @param month
     * @param day
     * @param year
     * @param offsetHour
     * @param afterGMT
     * @return
     */
    public static DateTime toDateTimeWithOffset(int month, int day, int year, int offsetHour, boolean afterGMT) {
        final DateTime dt = new DateTime();
        dt.setDate(new Date((short) month, (short) day, (short) year));
        final TimeZone tz = new TimeZone();
        tz.setAfterUTC(afterGMT);
        tz.setHour((short) offsetHour);
        tz.setHourIsSet(true);
        final Time time = new Time().setHour((short) 1).setMinute((short) 1).setSecond((short) 1).setTz(tz);
        dt.setTime(time);
        return dt;
    }
}
