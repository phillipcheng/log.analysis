package etl.util;

import org.apache.parquet.example.data.simple.NanoTime;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class NanoTimeUtils {
    private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

    private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<Calendar>();
    private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<Calendar>();

    private static Calendar getGMTCalendar() {
        //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
        if (parquetGMTCalendar.get() == null) {
            parquetGMTCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
        }
        return parquetGMTCalendar.get();
    }

    private static Calendar getLocalCalendar() {
        if (parquetLocalCalendar.get() == null) {
            parquetLocalCalendar.set(Calendar.getInstance());
        }
        return parquetLocalCalendar.get();
    }

    private static Calendar getCalendar(boolean skipConversion) {
        Calendar calendar = skipConversion ? getLocalCalendar() : getGMTCalendar();
        calendar.clear(); // Reset all fields before reusing this instance
        return calendar;
    }

    private static class JulianDateStamp {
        /**
         * Integer part of the Julian Date (JD).
         */
        protected int integer;

        /**
         * Returns integer part of the Julian Date (JD).
         */
        public int getInteger() {
            return integer;
        }

        /**
         * Fraction part of the Julian Date (JD).
         * Should be always in [0.0, 1.0) range.
         */
        protected double fraction;

        /**
         * Returns the fraction part of Julian Date (JD).
         * Returned value is always in [0.0, 1.0) range.
         */
        public double getFraction() {
            return fraction;
        }

        /**
         * Returns JDN. Note that JDN is not equal to {@link #integer}. It is calculated by
         * rounding to the nearest integer.
         */
        public int getJulianDayNumber() {
            if (fraction >= 0.5) {
                return integer + 1;
            }
            return integer;
        }

        /**
         * Returns current year.
         */
        public int getYear() {
            int year, month, day;
            double frac;
            int jd, ka, kb, kc, kd, ke, ialp;

            //double JD = jds.doubleValue();//jdate;
            //jd = (int)(JD + 0.5);							// integer julian date
            //frac = JD + 0.5 - (double)jd + 1.0e-10;		// day fraction

            ka = (int)(this.fraction + 0.5);
            jd = this.integer + ka;
            frac = this.fraction + 0.5 - ka + 1.0e-10;

            ka = jd;
            if (jd >= 2299161) {
                ialp = (int)(((double)jd - 1867216.25) / (36524.25));
                ka = jd + 1 + ialp - (ialp >> 2);
            }
            kb = ka + 1524;
            kc =  (int)(((double)kb - 122.1) / 365.25);
            kd = (int)((double)kc * 365.25);
            ke = (int)((double)(kb - kd) / 30.6001);
            day = kb - kd - ((int)((double)ke * 30.6001));
            if (ke > 13) {
                month = ke - 13;
            } else {
                month = ke - 1;
            }
            if ((month == 2) && (day > 28)){
                day = 29;
            }
            if ((month == 2) && (day == 29) && (ke == 3)) {
                year = kc - 4716;
            } else if (month > 2) {
                year = kc - 4716;
            } else {
                year = kc - 4715;
            }
            return year;
        }

        /**
         * Returns current month.
         */
        public int getMonth() {
            int year, month, day;
            double frac;
            int jd, ka, kb, kc, kd, ke, ialp;

            //double JD = jds.doubleValue();//jdate;
            //jd = (int)(JD + 0.5);							// integer julian date
            //frac = JD + 0.5 - (double)jd + 1.0e-10;		// day fraction

            ka = (int)(this.fraction + 0.5);
            jd = this.integer + ka;
            frac = this.fraction + 0.5 - ka + 1.0e-10;

            ka = jd;
            if (jd >= 2299161) {
                ialp = (int)(((double)jd - 1867216.25) / (36524.25));
                ka = jd + 1 + ialp - (ialp >> 2);
            }
            kb = ka + 1524;
            kc =  (int)(((double)kb - 122.1) / 365.25);
            kd = (int)((double)kc * 365.25);
            ke = (int)((double)(kb - kd) / 30.6001);
            day = kb - kd - ((int)((double)ke * 30.6001));
            if (ke > 13) {
                month = ke - 13;
            } else {
                month = ke - 1;
            }
            if ((month == 2) && (day > 28)){
                day = 29;
            }
            if ((month == 2) && (day == 29) && (ke == 3)) {
                year = kc - 4716;
            } else if (month > 2) {
                year = kc - 4716;
            } else {
                year = kc - 4715;
            }
            return month;
        }

        /**
         * Returns current day of month.
         * @see #getDayOfMonth
         */
        public int getDay() {
            int year, month, day;
            double frac;
            int jd, ka, kb, kc, kd, ke, ialp;

            //double JD = jds.doubleValue();//jdate;
            //jd = (int)(JD + 0.5);							// integer julian date
            //frac = JD + 0.5 - (double)jd + 1.0e-10;		// day fraction

            ka = (int)(this.fraction + 0.5);
            jd = this.integer + ka;
            frac = this.fraction + 0.5 - ka + 1.0e-10;

            ka = jd;
            if (jd >= 2299161) {
                ialp = (int)(((double)jd - 1867216.25) / (36524.25));
                ka = jd + 1 + ialp - (ialp >> 2);
            }
            kb = ka + 1524;
            kc =  (int)(((double)kb - 122.1) / 365.25);
            kd = (int)((double)kc * 365.25);
            ke = (int)((double)(kb - kd) / 30.6001);
            day = kb - kd - ((int)((double)ke * 30.6001));
            if (ke > 13) {
                month = ke - 13;
            } else {
                month = ke - 1;
            }
            if ((month == 2) && (day > 28)){
                day = 29;
            }
            if ((month == 2) && (day == 29) && (ke == 3)) {
                year = kc - 4716;
            } else if (month > 2) {
                year = kc - 4716;
            } else {
                year = kc - 4715;
            }
            return day;
        }

        /**
         * Creates JD from both integer and fractional part using normalization.
         * Normalization occurs when fractional part is out of range.
         *
         * @param i integer part
         * @param f fractional part should be in range [0.0, 1.0)
         * @see #set(int, double)
         */
        public JulianDateStamp(int i, double f) {
            set(i, f);
        }

        /**
         * Creates JD from a <code>double</code>.
         */
        public JulianDateStamp(double jd) {
            set(jd);
        }

        /**
         * Sets integer and fractional part with normalization.
         * Normalization means that if double is out of range,
         * values will be correctly fixed.
         */
        public void set(int i, double f) {
            integer = i;
            int fi = (int) f;
            f -= fi;
            integer += fi;
            if (f < 0) {
                f += 1;
                integer--;
            }
            this.fraction = f;
        }

        public void set(double jd) {
            integer = (int)jd;
            fraction = jd - (double)integer;
        }
    }

    private static JulianDateStamp toJulianDate(int year, int month, int day, int hour, int minute, int second, int millisecond) {
        // month range fix
        if ((month > 12) || (month < -12)) {
            month--;
            int delta = month / 12;
            year += delta;
            month -= delta * 12;
            month++;
        }
        if (month < 0) {
            year--;
            month += 12;
        }

        // decimal day fraction
        double frac = (hour / 24.0) + (minute / 1440.0) + (second / 86400.0) + (millisecond / 86400000.0);
        if (frac < 0) {        // negative time fix
            int delta = ((int) (-frac)) + 1;
            frac += delta;
            day -= delta;
        }
        //double gyr = year + (0.01 * month) + (0.0001 * day) + (0.0001 * frac) + 1.0e-9;
        double gyr = year + (0.01 * month) + (0.0001 * (day + frac)) + 1.0e-9;

        // conversion factors
        int iy0;
        int im0;
        if (month <= 2) {
            iy0 = year - 1;
            im0 = month + 12;
        } else {
            iy0 = year;
            im0 = month;
        }
        int ia = iy0 / 100;
        int ib = 2 - ia + (ia >> 2);

        // calculate julian date
        int jd;
        if (year <= 0) {
            jd = (int) ((365.25 * iy0) - 0.75) + (int) (30.6001 * (im0 + 1)) + day + 1720994;
        } else {
            jd = (int) (365.25 * iy0) + (int) (30.6001 * (im0 + 1)) + day + 1720994;
        }
        if (gyr >= 1582.1015) {                        // on or after 15 October 1582
            jd += ib;
        }
        //return  jd + frac + 0.5;

        return new JulianDateStamp(jd, frac + 0.5);
    }

    public static NanoTime getNanoTime(long timestamp, boolean skipConversion) {
        Timestamp ts = new Timestamp(timestamp);
        Calendar calendar = getCalendar(skipConversion);
        calendar.setTime(ts);
        int year = calendar.get(Calendar.YEAR);
        if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }
        JulianDateStamp jDateTime = toJulianDate(year,
                calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
                calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0, 0);
        int days = jDateTime.getJulianDayNumber();

        long hour = calendar.get(Calendar.HOUR_OF_DAY);
        long minute = calendar.get(Calendar.MINUTE);
        long second = calendar.get(Calendar.SECOND);
        long nanos = ts.getNanos();
        long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
                NANOS_PER_HOUR * hour;

        return new NanoTime(days, nanosOfDay);
    }

    public static long getTimestamp(NanoTime nt, boolean skipConversion) {
        int julianDay = nt.getJulianDay();
        long nanosOfDay = nt.getTimeOfDayNanos();

        long remainder = nanosOfDay;
        julianDay += remainder / NANOS_PER_DAY;
        remainder %= NANOS_PER_DAY;
        if (remainder < 0) {
            remainder += NANOS_PER_DAY;
            julianDay--;
        }

        JulianDateStamp jDateTime = new JulianDateStamp((double) julianDay);
        Calendar calendar = getCalendar(skipConversion);
        calendar.set(Calendar.YEAR, jDateTime.getYear());
        calendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calendar index starting at 1.
        calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

        int hour = (int) (remainder / (NANOS_PER_HOUR));
        remainder = remainder % (NANOS_PER_HOUR);
        int minutes = (int) (remainder / (NANOS_PER_MINUTE));
        remainder = remainder % (NANOS_PER_MINUTE);
        int seconds = (int) (remainder / (NANOS_PER_SECOND));
        long nanos = remainder % NANOS_PER_SECOND;

        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);
        return calendar.getTimeInMillis();
    }
}
