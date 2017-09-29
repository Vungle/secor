package com.pinterest.secor.parser;

import com.google.common.net.HostAndPort;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.timgroup.statsd.NonBlockingStatsDClient;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by foolim on 8/12/15.
 */
public class FooJsonDateTimeMessageParser extends JsonMessageParser {

    private static final Logger LOG = LoggerFactory.getLogger(FooJsonDateTimeMessageParser.class);
    private static DateTimeFormatter badAdServerFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static NonBlockingStatsDClient mStatsDClient;

    public FooJsonDateTimeMessageParser(SecorConfig config) {
        super(config);
        if (mConfig.getStatsDHostPort() != null && !mConfig.getStatsDHostPort().isEmpty()) {
            HostAndPort hostPort = HostAndPort.fromString(mConfig.getStatsDHostPort());
            mStatsDClient = new NonBlockingStatsDClient(null, hostPort.getHostText(), hostPort.getPort());
        }
    }

    @Override
    public long extractTimestampMillis(final Message message) {

        JSONObject jsonObject;
        // step 1: parse the message into json object
        try {
            jsonObject = (JSONObject) (new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE)).parse(message.getPayload());
        } catch (Throwable e) {
            LOG.error("FooJsonDateTimeMessageParser: Invalid Json: " + message, e);
            if (mStatsDClient != null) {
                mStatsDClient.incrementCounter("secor.kafka." + message.getTopic().toLowerCase() + ".failed.json_parsing.count");
            }

            // send the bad messages back to 1970s
            return 0;

        }

        LOG.debug("FooJsonDateTimeMessageParser: JsonObject: " + message);

        // step 2: extract the value from timestamp field
        Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
        if (fieldValue == null) {
            LOG.error("FooJsonDateTimeMessageParser: Missing time field: " + jsonObject.toJSONString());
            if (mStatsDClient != null) {
                mStatsDClient.incrementCounter("secor.kafka." + message.getTopic().toLowerCase() + ".failed.date_missing.count");
            }

            // send the bad messages back to 1970s
            return 0;
        }

        // step 3: try to stringify and parse the timestamp value
        try {
            String timestampValue = fieldValue.toString();
            long ts;
            if (timestampValue.contains("T")) {
                ts = DateTime.parse(timestampValue).toInstant().getMillis();
                if (mStatsDClient != null) {
                    mStatsDClient.incrementCounter("secor.kafka." + message.getTopic().toLowerCase() + "succeeded.date_good.count");
                }
            } else {
                ts = badAdServerFormat.parseDateTime(timestampValue).toInstant().getMillis();
                if (mStatsDClient != null) {
                    mStatsDClient.incrementCounter("secor.kafka." + message.getTopic().toLowerCase() + "succeeded.date_bad.count");
                }
            }
            return ts;
        } catch (Throwable e) {
            LOG.error("FooJsonDateTimeMessageParser: Invalid TimeStamp: " + message, e);
            if (mStatsDClient != null) {
                mStatsDClient.incrementCounter("secor.kafka." + message.getTopic().toLowerCase() + ".failed.date_parsing.count");
            }

            // send the bad messages back to 1970s
            return 0;
        }


    }
}
