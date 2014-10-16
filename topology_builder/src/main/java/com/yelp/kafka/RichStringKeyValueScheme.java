package com.yelp.kafka;

import java.util.List;

import backtype.storm.tuple.Fields;
import storm.kafka.StringKeyValueScheme;

public class RichStringKeyValueScheme extends StringKeyValueScheme {
	public static String TOPIC_KEY = "topic";
	public String topic_name;

	public RichStringKeyValueScheme(String topic_name) {
		super();
		this.topic_name = topic_name;
	}

	@Override
	public java.util.List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
		List<Object> deserialized = super.deserializeKeyAndValue(key, value);
		deserialized.add(topic_name);
		return deserialized;
	};

	@Override
	public Fields getOutputFields() {
		return new Fields(STRING_SCHEME_KEY, TOPIC_KEY);
	}
}
