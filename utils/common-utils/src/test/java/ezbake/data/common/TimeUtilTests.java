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

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.Test;

import ezbake.base.thrift.Date;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.Time;
import ezbake.base.thrift.TimeZone;

public class TimeUtilTests {

    @Test
    public void testGetCurrentDate() {
        final Calendar current = Calendar.getInstance();
        final DateTime dateTime = TimeUtil.getCurrentThriftDateTime();

        assertEquals(current.get(Calendar.YEAR), dateTime.getDate().getYear());
        assertEquals(current.get(Calendar.MONTH) + 1, dateTime.getDate().getMonth());
        assertEquals(current.get(Calendar.DAY_OF_MONTH), dateTime.getDate().getDay());
        assertEquals(current.get(Calendar.HOUR_OF_DAY), dateTime.getTime().getHour());
        assertEquals(current.get(Calendar.MINUTE), dateTime.getTime().getMinute());
        assertEquals(current.get(Calendar.SECOND), dateTime.getTime().getSecond());
    }

    @Test
    public void testConvertDateTime() {
        final Calendar special = new GregorianCalendar(1987, 0, 12, 5, 32, 15);
        final DateTime dateTime = TimeUtil.convertToThriftDateTime(special.getTimeInMillis());

        assertEquals(special.get(Calendar.YEAR), dateTime.getDate().getYear());
        assertEquals(special.get(Calendar.MONTH) + 1, dateTime.getDate().getMonth());
        assertEquals(special.get(Calendar.DAY_OF_MONTH), dateTime.getDate().getDay());
        assertEquals(special.get(Calendar.HOUR_OF_DAY), dateTime.getTime().getHour());
        assertEquals(special.get(Calendar.MINUTE), dateTime.getTime().getMinute());
        assertEquals(special.get(Calendar.SECOND), dateTime.getTime().getSecond());
    }

    @Test
    public void testConvertFromThriftDateTime() {
        final DateTime dateTime = new DateTime();
        final Date date = new Date((short) 2, (short) 12, (short) 2014);

        final Time time = new Time();
        time.setHour((short) 20);
        time.setMinute((short) 14);
        time.setSecond((short) 24);
        time.setMillisecond((short) 115);

        final TimeZone tz = new TimeZone((short) 5, (short) 0, false);
        time.setTz(tz);

        dateTime.setDate(date);
        dateTime.setTime(time);

        final long millis = TimeUtil.convertFromThriftDateTime(dateTime);
        assertEquals("Time in millis is correct", 1392254064115l, millis);
    }

    @Test
    public void testConvertFromThriftDateTime_JustDate() {
        final DateTime dateTime = new DateTime();
        final Date date = new Date((short) 2, (short) 12, (short) 2014);

        dateTime.setDate(date);

        final long millis = TimeUtil.convertFromThriftDateTime(dateTime);
        assertEquals("Time in millis is correct", 1392163200000l, millis);
    }

    @Test
    public void testConvertFromThriftDateTime_AfterUTC() {
        final DateTime dateTime = new DateTime();
        final Date date = new Date((short) 2, (short) 13, (short) 2014);

        final Time time = new Time();
        time.setHour((short) 7);
        time.setMinute((short) 14);
        time.setSecond((short) 24);
        time.setMillisecond((short) 115);

        final TimeZone tz = new TimeZone((short) 6, (short) 0, true);
        time.setTz(tz);

        dateTime.setDate(date);
        dateTime.setTime(time);

        final long millis = TimeUtil.convertFromThriftDateTime(dateTime);
        assertEquals("Time in millis is correct", 1392254064115l, millis);
    }
}
