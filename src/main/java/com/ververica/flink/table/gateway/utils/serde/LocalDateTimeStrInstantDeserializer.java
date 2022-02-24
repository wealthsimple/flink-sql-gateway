package com.ververica.flink.table.gateway.utils.serde;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 *  simply deserialize local date time str to Instant.
 */
public class LocalDateTimeStrInstantDeserializer extends JsonDeserializer<Instant> {

	DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault());

	@Override
	public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
		return Instant.from(formatter.parse(parser.getText()));
	}

}
