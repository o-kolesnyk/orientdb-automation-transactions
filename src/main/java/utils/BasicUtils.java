package utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicUtils {

    private static final int MIN_BATCH = 6;
    private static final int MAX_BATCH = 27;
    private static final int HOURS_NUMBER = 4;

    public static Date getTimeToInterrupt() {
        Date date = new Date();
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        //TODO: change minutes to hours and use HOURS_NUMBER
        calendar.add(Calendar.MINUTE, 1);
        date.setTime(calendar.getTime().getTime());
        return date;
    }

    public static int generateBatchSize() {
        return ThreadLocalRandom.current().nextInt(MIN_BATCH, MAX_BATCH);
    }

}
