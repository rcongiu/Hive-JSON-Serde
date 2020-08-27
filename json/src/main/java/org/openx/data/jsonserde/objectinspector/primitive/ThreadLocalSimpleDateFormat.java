package org.openx.data.jsonserde.objectinspector.primitive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Wraps a {@link SimpleDateFormat} instance with a {@link ThreadLocal} since the former is not thread safe and
 * mutates more than a few underlying fields directly during parsing. When used from multiple fields, this can
 * corrupt data in a way that may or may not cause exceptions (ie: can cause silent corruption).
 */
final class ThreadLocalSimpleDateFormat {
    private final ThreadLocal<SimpleDateFormat> threadLocal;

    public ThreadLocalSimpleDateFormat(final String pattern) {
        this(pattern, null);
    }

    public ThreadLocalSimpleDateFormat(final String pattern, final TimeZone timeZone) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(pattern);
            if (timeZone != null) {
                format.setTimeZone(timeZone);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create ThreadLocalSimpleDateFormat with pattern: " + pattern, e);
        }
        this.threadLocal = new ThreadLocal<SimpleDateFormat>() {
            @Override
            public SimpleDateFormat initialValue() {
                SimpleDateFormat format = new SimpleDateFormat(pattern);
                if (timeZone != null) {
                    format.setTimeZone(timeZone);
                }
                return format;
            }
        };
    }

    public Date parse(String source) throws ParseException {
        return threadLocal.get().parse(source);
    }

    public String format(Date date) {
        return threadLocal.get().format(date);
    }

    public String format(long value) {
        return threadLocal.get().format(value);
    }
}
