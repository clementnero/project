package com.atos.flume;

import com.google.common.base.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.atos.flume.MsmRegexExtractInterceptor.Constants.*;

/**
 * Created by $cnero on 16-2-10.
 */
public class MsmRegexExtractInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory
            .getLogger(MsmRegexExtractInterceptor.class);

    private final Charset charset;
    private final Pattern pattern;

    /**
     *
     */
    private MsmRegexExtractInterceptor(Charset charset, String pattern) {
        this.charset = charset;
        this.pattern = Pattern.compile(pattern,Pattern.DOTALL | Pattern.MULTILINE);
    }

    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String msg = new String(event.getBody(), charset);

        logger.debug(String.format("MsmRegexExtractInterceptor original body=%s",msg));

        Matcher matcher = this.pattern.matcher(msg);

        final List<String> tagValues = new ArrayList<String>();
        while (matcher.find() && matcher.groupCount() > 0) {
            tagValues.add(matcher.group(1));
        }

        if (tagValues.size() > 0) {
            msg = StringUtils.join(tagValues,"\n");
        }

        logger.debug(String.format("MsmRegexExtractInterceptor extract body=%s",msg));
        event.setBody(msg.getBytes());

        // set header based on the message type (for demonstration purpose)
        Matcher typeMatcher = Pattern.compile("<repd:IdPRM>([0-9]+)</repd:IdPRM>", Pattern.DOTALL | Pattern.MULTILINE).matcher(msg);
        String messageType = (typeMatcher.find() && typeMatcher.groupCount() > 0)?
                ((Integer.parseInt(typeMatcher.group(1)))%2 == 0?"TypeA":"TypeB"):
                "unknown";

        headers.put("type-id",messageType);

        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events to intercept
     * @return events
     */
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private Charset charset = Charsets.UTF_8;
        private String pattern = PATTERN_DEFAULT;

        public void configure(Context context) {
            // May throw IllegalArgumentException for unsupported charsets.
            charset = Charset.forName(context.getString(CHARSET_KEY, CHARSET_DEFAULT));
            pattern = context.getString(PATTERN_KEY, PATTERN_DEFAULT);
        }

        public Interceptor build() {
            logger.info(String.format(
                    "Creating MsmRegexExtractInterceptor: charset=%s, pattern=%s",
                    charset,pattern));
            return new MsmRegexExtractInterceptor(charset,pattern);
        }

    }

    public static class Constants {
        public static final String CHARSET_KEY = "charset";
        public static final String CHARSET_DEFAULT = "UTF-8";

        public static final String PATTERN_KEY = "pattern";
        public static final String PATTERN_DEFAULT = "(.*)";
    }

}
