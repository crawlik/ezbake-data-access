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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ezbake.base.thrift.DateTime;

/**
 *
 * @author blong
 */
public class DateTimeConverterTest {

    private final static TimeZone SYSTEM_DEFAULT_TIMEZONE = TimeZone.getDefault(), UTC_TIMEZONE = TimeZone
            .getTimeZone("UTC"), GMT_TIMEZONE = TimeZone.getTimeZone("GMT"), NY_TIMEZONE = TimeZone
            .getTimeZone("America/New_York"), CALCUTTA_TIMEZONE = TimeZone.getTimeZone("Asia/Calcutta"),
            LOS_ANGELES_TIMEZONE = TimeZone.getTimeZone("America/Los_Angeles");
    private final static int UTC_OFFSET_MILLISECONDS = 0;

    // 1 hour * 60 minutes * 60 seconds * 1000 milliseconds
    private final static int MILLISECONDS_IN_ONE_HOUR = 1 * 60 * 60 * 1000;
    SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private static Calendar calendarAtEpoch;
    private Date dateAtEpoch;

    public DateTimeConverterTest() {}

    @Before
    public void setUp() {
        // Calendar can't be constructed with a specific date & time
        // See: http://stackoverflow.com/a/3978229/320399
        calendarAtEpoch = Calendar.getInstance();
        // Instead we create it, then set it's value (here the epoch)
        calendarAtEpoch.setTimeInMillis(0L);
        // Date can be constructed at the epoch
        dateAtEpoch = new Date(0L);
    }

    @After
    public void tearDown() {
        // Reset the system default timezone
        TimeZone.setDefault(SYSTEM_DEFAULT_TIMEZONE);
    }

    private void announceTest(String test) {
        System.out.println("\n\n-- " + test + " --");
    }

    /**
     * Sanity check.
     *
     * This method tests various features of the built-in Java Date & Calendar types.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testJavaDateAndCalendar() {
        announceTest("testJavaDateAndCalendar");

        // UTC does not observe daylight savings time
        // TODO: Java 7 only
        // assertEquals(false, UTC_TIMEZONE.observesDaylightTime());
        assertEquals(false, UTC_TIMEZONE.useDaylightTime());

        // GMT does not observe daylight savings time
        // TODO: Java 7 only
        // assertEquals(false, GMT_TIMEZONE.observesDaylightTime());
        assertEquals(false, GMT_TIMEZONE.useDaylightTime());

        // In New York, daylight savings is observed
        // TODO: Java 7 only
        // assertEquals(false, NY_TIMEZONE.observesDaylightTime());
        assertEquals(true, NY_TIMEZONE.useDaylightTime());

        // The raw offset for UTC & GMT is always the same
        assertEquals(UTC_TIMEZONE.getRawOffset(), GMT_TIMEZONE.getRawOffset());
        // The raw offset for UTC is never the same as New York
        assertTrue(UTC_TIMEZONE.getRawOffset() != NY_TIMEZONE.getRawOffset());

        // ********************************************
        // Test some of the features of java Calendar
        // ********************************************

        // The calendar we've created will use an unknown timezone (the system default)
        // It's unlikely that this JVM is running at GMT or UTC
        assertTrue(!UTC_TIMEZONE.equals(calendarAtEpoch.getTimeZone()));
        assertTrue(!GMT_TIMEZONE.equals(calendarAtEpoch.getTimeZone()));
        // However, changing the timezone of the Calendar shouldn't change it's
        // underlying value (milliseconds from epoch)
        calendarAtEpoch.setTimeZone(LOS_ANGELES_TIMEZONE);
        assertEquals(calendarAtEpoch.getTimeInMillis(), 0L);

        // ********************************************
        // Test some of the features of java Date
        // ********************************************
        assertEquals(dateAtEpoch.getTime(), 0L);

        // Unlike Calendar, there is no specific TimeZone used internally for java.util.Date.
        // See here: http://stackoverflow.com/a/308689/320399
        // Java Date only uses a Timezone for printing.
        // A default is used when calling date.toString()
        System.out.println("Invoking 'dateAtEpoch.toString()': " + dateAtEpoch.toString() + "\n");

        final DateFormat dateFormatter = DateFormat.getInstance();

        // Compare the timezone in the default formatter to the system TZ
        final TimeZone defaultFormatterTimezone = dateFormatter.getTimeZone();
        assertEquals(SYSTEM_DEFAULT_TIMEZONE, defaultFormatterTimezone);
        System.out.println("Date in default format: " + dateFormatter.format(dateAtEpoch));

        dateFormatter.setTimeZone(UTC_TIMEZONE);
        System.out.println("Date formatted for UTC: " + dateFormatter.format(dateAtEpoch));

        dateFormatter.setTimeZone(GMT_TIMEZONE);
        System.out.println("Date formatted for GMT: " + dateFormatter.format(dateAtEpoch));

        dateFormatter.setTimeZone(NY_TIMEZONE);
        System.out.println("Date formatted for NY: " + dateFormatter.format(dateAtEpoch));

        // Changing timezone never changes the underlying value
        assertEquals(dateAtEpoch.getTime(), 0L);

        // Only calling the (Date & time) setters affects the underlying value
        dateAtEpoch.setMinutes(55);
        assertEquals(dateAtEpoch.getTime(), 55 * 60 * 1000L);
        dateAtEpoch.setMinutes(0);

    }

    /**
     * Test of getOffsetHoursAndMinutes method, of class DateTimeConverter.
     */
    @Test
    public void testGetOffsetHoursAndMinutes() {
        announceTest("getOffsetHoursAndMinutes");

        final String epochDateAtGMTOffset = DateTimeConverter.getOffsetHoursAndMinutes(dateAtEpoch, GMT_TIMEZONE);
        final String epochCalAtGMTOffset =
                DateTimeConverter.getOffsetHoursAndMinutes(calendarAtEpoch.getTime(), GMT_TIMEZONE);

        System.out.println("Offset for dateAtEpoch in GMT: \t\t" + epochDateAtGMTOffset);
        System.out.println("Offset for calendarAtEpoch in GMT: \t" + epochCalAtGMTOffset);
        assertEquals(epochDateAtGMTOffset, epochCalAtGMTOffset);

        String expected = "+00:00", actualResult;
        assertEquals(expected, epochDateAtGMTOffset);
        assertEquals(expected, epochCalAtGMTOffset);

        // hours * minutes * seconds & milliseconds
        final int gmtMinus5HoursOffset = -5 * MILLISECONDS_IN_ONE_HOUR;
        final String[] zonesInGMTMinus5 = TimeZone.getAvailableIDs(gmtMinus5HoursOffset);
        assertTrue(zonesInGMTMinus5.length > 0);
        final TimeZone someMinus5TimeZone = TimeZone.getTimeZone(zonesInGMTMinus5[0]);
        System.out.println("Running a test against timezone: '" + someMinus5TimeZone.getID()// .getDisplayName()
                + "'");
        expected = "-05:00";
        actualResult = DateTimeConverter.getOffsetHoursAndMinutes(dateAtEpoch, someMinus5TimeZone);
        assertEquals(expected, actualResult);

        // hours * minutes * seconds & milliseconds
        final int gmtPlus3HoursOffset = +3 * MILLISECONDS_IN_ONE_HOUR;
        final String[] zonesInGMTPlus3 = TimeZone.getAvailableIDs(gmtPlus3HoursOffset);
        assertTrue(zonesInGMTPlus3.length > 0);
        final TimeZone somePlus3TimeZone = TimeZone.getTimeZone(zonesInGMTPlus3[0]);
        System.out.println("Running a test against timezone: '" + somePlus3TimeZone.getID()// .getDisplayName()
                + "'");
        expected = "+03:00";
        actualResult = DateTimeConverter.getOffsetHoursAndMinutes(calendarAtEpoch.getTime(), somePlus3TimeZone);
        assertEquals(expected, actualResult);
    }

    /**
     * Test of getOffsetHours method, of class DateTimeConverter.
     */
    @Test
    public void testGetOffsetHours_Date_TimeZone() {
        announceTest("getOffsetHours");

        final String epochDateAtGMTOffset = DateTimeConverter.getOffsetHours(dateAtEpoch, GMT_TIMEZONE);
        final String epochCalAtGMTOffset = DateTimeConverter.getOffsetHours(calendarAtEpoch.getTime(), GMT_TIMEZONE);

        System.out.println("Offset for dateAtEpoch in GMT: \t\t" + epochDateAtGMTOffset);
        System.out.println("Offset for calendarAtEpoch in GMT: \t" + epochCalAtGMTOffset);
        assertEquals(epochDateAtGMTOffset, epochCalAtGMTOffset);

        String expected = "+00", actualResult;
        assertEquals(expected, epochDateAtGMTOffset);
        assertEquals(expected, epochCalAtGMTOffset);

        // hours * minutes * seconds & milliseconds
        final int gmtMinus5HoursOffset = -5 * MILLISECONDS_IN_ONE_HOUR;
        final String[] zonesInGMTMinus5 = TimeZone.getAvailableIDs(gmtMinus5HoursOffset);
        assertTrue(zonesInGMTMinus5.length > 0);
        final TimeZone someMinus5TimeZone = TimeZone.getTimeZone(zonesInGMTMinus5[0]);
        System.out.println("Running a test against timezone: '" + someMinus5TimeZone.getID()// .getDisplayName()
                + "'");
        expected = "-05";
        actualResult = DateTimeConverter.getOffsetHours(dateAtEpoch, someMinus5TimeZone);
        assertEquals(expected, actualResult);

        // hours * minutes * seconds & milliseconds
        final int gmtPlus7HoursOffset = +7 * MILLISECONDS_IN_ONE_HOUR;
        final String[] zonesInGMTPlus7 = TimeZone.getAvailableIDs(gmtPlus7HoursOffset);
        assertTrue(zonesInGMTPlus7.length > 0);
        final TimeZone somePlus7TimeZone = TimeZone.getTimeZone(zonesInGMTPlus7[0]);
        System.out.println("Running a test against timezone: '" + somePlus7TimeZone.getID()// .getDisplayName()
                + "'");
        expected = "+07";
        actualResult = DateTimeConverter.getOffsetHours(calendarAtEpoch.getTime(), somePlus7TimeZone);
        assertEquals(expected, actualResult);
    }

    /**
     * Test of getOffsetHours method, of class DateTimeConverter.
     */
    @Ignore
    public void testGetOffsetHours_0args() {
        announceTest("getOffsetHours");
        final String expResult = "";
        final String result = DateTimeConverter.getOffsetHours();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of transformDateTime method, of class DateTimeConverter.
     */
    @Ignore
    public void testTransformDateTime() {
        announceTest("transformDateTime");
        final DateTime dt = null;
        final Calendar expResult = null;
        final Calendar result = DateTimeConverter.transformDateTime(dt);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of toCalendar method, of class DateTimeConverter.
     */
    @Test
    public void testToCalendar() {
        announceTest("toCalendar");
        final int year = 2014;
        // January is zero-based within the GregorianCalendar constructor
        final int month = 0;
        final int day = 7;
        final int hour = 15;
        final int minute = 41;
        final int second = 1;

        // Get SYSTEM DEFAULT offset in hours
        final int zoneOffsetHoursOnly = SYSTEM_DEFAULT_TIMEZONE.getRawOffset() / MILLISECONDS_IN_ONE_HOUR;

        // The GregorianCalendar constructor will use the system default timezone
        final Calendar expectedResult = new GregorianCalendar(year, month, day, hour, minute, second);
        final Calendar actualResult =
                DateTimeConverter.toCalendar(year, month + 1, day, hour, minute, second, zoneOffsetHoursOnly);

        // FIXME: Why doesn't this work?
        // assertEquals(expectedResult, actualResult);
        // Workaround
        assertEquals(expectedResult.getTime(), actualResult.getTime());
    }

    private String toIso8601(Date javaDate) {
        return ISO8601DATEFORMAT.format(javaDate);
    }

    /**
     * Test of toCalendar method, of class DateTimeConverter.
     */
    @Test
    public void testToCalendar2() {
        announceTest("testToCalendar2");

        // Month in the java.util.Calendar is is 0-indexed. Here we use '1' (Febuary)
        final Calendar expectedResult = new GregorianCalendar(2014, 1, 2, 12, 26, 42);

        final int year = expectedResult.get(Calendar.YEAR);
        final int zeroIndexedMonth = expectedResult.get(Calendar.MONTH);
        final int day = expectedResult.get(Calendar.DAY_OF_MONTH);
        final int hour = expectedResult.get(Calendar.HOUR_OF_DAY);
        final int minute = expectedResult.get(Calendar.MINUTE);
        final int second = expectedResult.get(Calendar.SECOND);

        final int offset = expectedResult.get(Calendar.ZONE_OFFSET) / MILLISECONDS_IN_ONE_HOUR;

        // int offset = offsetHours(new Date(expResult.getTimeInMillis()));
        System.out.println("Offset of input (expected) date: " + offset);
        final Calendar actualResult =
                DateTimeConverter.toCalendar(year, zeroIndexedMonth + 1, day, hour, minute, second, offset);

        assertEquals(expectedResult.getTime(), actualResult.getTime());

        System.out.println("Expected result as Date: '" + toIso8601(expectedResult.getTime()) + "'");
        System.out.println("Actual result as Date:\t '" + toIso8601(actualResult.getTime()) + "'");

        // Test some formatting
        assertEquals(toIso8601(expectedResult.getTime()), toIso8601(actualResult.getTime()));
    }

    private static int getTimeZoneOffsetMsFromDt(DateTime dateTime) {
        final ezbake.base.thrift.TimeZone timezone = dateTime.getTime().getTz();
        final short timezoneOffsetHours = timezone.getHour();
        final int offsetHoursInMins = timezoneOffsetHours * 60;
        final short timezoneOffsetMins = timezone.getMinute();
        // total minutes * 60 seconds per minute * 1000 milliseconds per second
        final int timezoneOffsetInMilliseconds = (offsetHoursInMins + timezoneOffsetMins) * 60 * 1000;
        return timezoneOffsetInMilliseconds;
    }

    /**
     * Test of toDateTimeWithoutOffset method, of class DateTimeConverter.
     */
    @Test
    public void testToDateTimeWithoutOffset_6args() {
        announceTest("testToDateTimeWithoutOffset_6args");
        final int year = 2013;
        final int month = 12;
        final int day = 1;
        final int hour = 12;
        final int minute = 30;
        final int second = 45;

        //
        // Test w/ Calcutta's TimeZone
        //
        // Set the JVM timezone to Calcutta, India
        TimeZone.setDefault(CALCUTTA_TIMEZONE);
        // Create a DateTime (while TZ set to Calcutta)
        final DateTime calcuttaDatetime =
                DateTimeConverter.toDateTimeWithoutOffset(year, month, day, hour, minute, second);
        // Calculate the offset (milliseconds) in the generated DateTime
        final int offsetReturned = getTimeZoneOffsetMsFromDt(calcuttaDatetime);

        // The returned DateTime should NOT have an offset
        assertEquals(UTC_OFFSET_MILLISECONDS, offsetReturned);

        // The offset (milliseconds) for the given timezone
        final int calcuttaRawOffst = CALCUTTA_TIMEZONE.getRawOffset();
        assertTrue(offsetReturned != calcuttaRawOffst);

        //
        // Test w/ L.A.'s TimeZone
        //
        // Set the JVM timezone to Los Angeles
        TimeZone.setDefault(LOS_ANGELES_TIMEZONE);
        // Create a DateTime (while TZ set to Los Angeles)
        final DateTime laDatetime = DateTimeConverter.toDateTimeWithoutOffset(year, month, day, hour, minute, second);
        // Calculate the offset (milliseconds) in the generated DateTime
        final int laOffsetReturned = getTimeZoneOffsetMsFromDt(laDatetime);

        // The returned DateTime should NOT have an offset
        assertEquals(UTC_OFFSET_MILLISECONDS, laOffsetReturned);

        // The offset (milliseconds) for the given timezone
        final int laRawOffst = LOS_ANGELES_TIMEZONE.getRawOffset();
        assertTrue(offsetReturned != laRawOffst);
    }

    /**
     * Test of toDateTimeWithoutOffset method, of class DateTimeConverter.
     */
    @Test
    public void testToDateTimeWithoutOffset_Date() {
        announceTest("testToDateTimeWithoutOffset_Date");

        final long fourDaysInMilliseconds = 4 * 24 * MILLISECONDS_IN_ONE_HOUR;

        // TimeZone.setDefault(laTimeZone);
        final Date someDate = new Date();
        final long someDateOffset = someDate.getTime();

        // TimeZone.setDefault(calcuttaTimeZone);
        final Date anotherDate = new Date(someDateOffset + fourDaysInMilliseconds);
        final long anotherDateOffset = anotherDate.getTime();

        // Milliseconds difference between 4 days
        final long dateDiff = anotherDateOffset - someDateOffset;

        // The offset between 2 dates created in the same
        // timezone, 4 days apart, should be 4 days time (in milliseconds).
        // NB: Since millisecond precision can be dicey, we're rounding
        // the milliseconds
        assertEquals(fourDaysInMilliseconds / 1000, dateDiff / 1000);

        final ezbake.base.thrift.DateTime someDateAsDateTime = DateTimeConverter.toDateTimeWithoutOffset(someDate);
        final ezbake.base.thrift.DateTime anotherDateAsDateTime =
                DateTimeConverter.toDateTimeWithoutOffset(anotherDate);

        // The returned TZ offset should be UTC
        final int someDateTimeOffset = getTimeZoneOffsetMsFromDt(someDateAsDateTime);
        assertEquals(UTC_OFFSET_MILLISECONDS, someDateTimeOffset);
        final int anotherDateTimeOffset = getTimeZoneOffsetMsFromDt(anotherDateAsDateTime);
        assertEquals(UTC_OFFSET_MILLISECONDS, anotherDateTimeOffset);

        // Account for month difference
        final Calendar anotherDateAsCal = Calendar.getInstance();
        anotherDateAsCal.clear();
        anotherDateAsCal.set(anotherDateAsDateTime.getDate().getYear(),
                anotherDateAsDateTime.getDate().getMonth() - 1, anotherDateAsDateTime.getDate().getDay());

        final Calendar someDateAsCal = Calendar.getInstance();
        someDateAsCal.clear();
        someDateAsCal.set(someDateAsDateTime.getDate().getYear(), someDateAsDateTime.getDate().getMonth() - 1,
                someDateAsDateTime.getDate().getDay());
        final long diffInMilliseconds = anotherDateAsCal.getTimeInMillis() - someDateAsCal.getTimeInMillis();
        assertEquals(fourDaysInMilliseconds, diffInMilliseconds);

        // Since we didn't specify a TimeZone on creation of
        // the ezbake.base.thrift.DateTime , it should be set to GMT
        final ezbake.base.thrift.TimeZone result1Tz = someDateAsDateTime.getTime().getTz();
        final ezbake.base.thrift.TimeZone result2Tz = anotherDateAsDateTime.getTime().getTz();

        assertEquals(result1Tz, result2Tz);

        // GMT offset is 0
        assertEquals(UTC_OFFSET_MILLISECONDS, UTC_TIMEZONE.getRawOffset());

        assertEquals(result1Tz.getHour(), 0);
        assertEquals(result1Tz.getMinute(), 0);

        // TODO: Research the data type, why is this false?
        // assertTrue(afterUtc);
    }

    /**
     * Test of toDateTimeWithOffset method, of class DateTimeConverter.
     */
    @Ignore
    public void testToDateTimeWithOffset() {
        announceTest("toDateTimeWithOffset");
        final int month = 0;
        final int day = 0;
        final int year = 0;
        final int offsetHour = 0;
        final boolean afterGMT = false;
        final DateTime expResult = null;
        final DateTime result = DateTimeConverter.toDateTimeWithOffset(month, day, year, offsetHour, afterGMT);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}
