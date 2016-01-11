package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
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

    public static DateTimeFormatter badAdServerFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public FooJsonDateTimeMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        LOG.debug("FooJsonDateTimeMessageParser: jsonObject: " + jsonObject.toJSONString());

        if (jsonObject != null) {
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
            if (fieldValue != null) {
                String timestampValue = fieldValue.toString();
                if (timestampValue.contains("T")) {
                    return DateTime.parse(timestampValue).toInstant().getMillis();
                } else {
                    return badAdServerFormat.parseDateTime(timestampValue).toInstant().getMillis();
                }
            }
        }
        return 0;
    }
}
