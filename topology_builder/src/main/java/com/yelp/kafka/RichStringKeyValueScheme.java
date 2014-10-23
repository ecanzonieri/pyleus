package com.yelp.kafka;

import java.util.Calendar;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.StringKeyValueScheme;
import storm.kafka.StringScheme;

public class RichStringKeyValueScheme extends StringKeyValueScheme {
    public static String TOPIC_KEY = "topic";
    public static String TIMESTAMP_KEY = "timestamp";
    public String topic_name;

    public RichStringKeyValueScheme(String topic_name) {
        super();
        this.topic_name = topic_name;
    }

    @Override
    public java.util.List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        if ( key == null ) {
            return deserialize(value);
        }
        String keyString = StringScheme.deserializeString(key);
        String valueString = StringScheme.deserializeString(value);
        return new Values(ImmutableMap.of(keyString, valueString),
                topic_name, Calendar.getInstance().getTimeInMillis());
    };

    // I need to override both deserialize and deserializeKeyAndValue because
    // generateTuples checks the message key and directly calls deserialize
    // if that is null.
    @Override
    public List<Object> deserialize(byte[] bytes) {
        return new Values(deserializeString(bytes),
                topic_name, Calendar.getInstance().getTimeInMillis());
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY, TOPIC_KEY, TIMESTAMP_KEY);
    }
}
