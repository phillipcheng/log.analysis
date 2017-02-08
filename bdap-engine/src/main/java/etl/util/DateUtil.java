package etl.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DateUtil {
	private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);
	private static final ThreadLocal<TimeZone> LOCAL_TIMEZONE = new ThreadLocal<TimeZone>() {
		@Override
		protected TimeZone initialValue() {
			return Calendar.getInstance().getTimeZone();
		}
	};
	private static final ThreadLocal<Calendar> UTC_CALENDAR = new ThreadLocal<Calendar>() {
		@Override
		protected Calendar initialValue() {
			return new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		}
	};
	private static final ThreadLocal<Calendar> LOCAL_CALENDAR = new ThreadLocal<Calendar>() {
		@Override
		protected Calendar initialValue() {
			return Calendar.getInstance();
		}
	};
	
	public static String getWeekOfDayForYesterday(){
		DateFormat dateFormat = new SimpleDateFormat("EEEE");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return (dateFormat.format(cal.getTime())).toLowerCase();
	}
	
	public static String getMonday(){
		return "monday";
	}

	public static long daysToMillis(int d) {
		return daysToMillis(d, true);
	}

	public static long daysToMillis(int d, boolean doesTimeMatter) {
		// What we are trying to get is the equivalent of new Date(ymd).getTime() in the local tz,
		// where ymd is whatever d represents. How it "works" is this.
		// First we get the UTC midnight for that day (which always exists, a small island of sanity).
		long utcMidnight = d * MILLIS_PER_DAY;
		// Now we take a local TZ offset at midnight UTC. Say we are in -4; that means (surprise
		// surprise) that at midnight UTC it was 20:00 in local. So far we are on firm ground.
		long utcMidnightOffset = LOCAL_TIMEZONE.get().getOffset(utcMidnight);
		// And now we wander straight into the swamp, when instead of adding, we subtract it from UTC
		// midnight to supposedly get local midnight (in the above case, 4:00 UTC). Of course, given
		// all the insane DST variations, where we actually end up is anyone's guess.
		long hopefullyMidnight = utcMidnight - utcMidnightOffset;
		// Then we determine the local TZ offset at that magical time.
		long offsetAtHM = LOCAL_TIMEZONE.get().getOffset(hopefullyMidnight);
		// If the offsets are the same, we assume our initial jump did not cross any DST boundaries,
		// and is thus valid. Both times flowed at the same pace. We congratulate ourselves and bail.
		if (utcMidnightOffset == offsetAtHM) return hopefullyMidnight;
		// Alas, we crossed some DST boundary. If the time of day doesn't matter to the caller, we'll
		// simply get the next day and go back half a day. This is not ideal but seems to work.
		if (!doesTimeMatter) return daysToMillis(d + 1) - (MILLIS_PER_DAY >> 1);
		// Now, we could get previous and next day, figure our how many hours were inserted or removed,
		// and from which of the days, etc. But at this point our gun is pointing straight at our foot,
		// so let's just go the safe, expensive way.
		Calendar utc = UTC_CALENDAR.get(), local = LOCAL_CALENDAR.get();
		utc.setTimeInMillis(utcMidnight);
		local.set(utc.get(Calendar.YEAR), utc.get(Calendar.MONTH), utc.get(Calendar.DAY_OF_MONTH));
		return local.getTimeInMillis();
	}

	private static int millisToDays(long millisLocal) {
		// We assume millisLocal is midnight of some date. What we are basically trying to do
		// here is go from local-midnight to UTC-midnight (or whatever time that happens to be).
		long millisUtc = millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal);
		int days;
		if (millisUtc >= 0L) {
			days = (int) (millisUtc / MILLIS_PER_DAY);
		} else {
			days = (int) ((millisUtc - 86399999 /*(MILLIS_PER_DAY - 1)*/) / MILLIS_PER_DAY);
		}
		return days;
	}

	public static int dateToDays(Date d) {
		// convert to equivalent time in UTC, then get day offset
		long millisLocal = d.getTime();
		return millisToDays(millisLocal);
	}

}
