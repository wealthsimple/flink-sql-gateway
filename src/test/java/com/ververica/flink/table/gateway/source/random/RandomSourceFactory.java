/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.gateway.source.random;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link TableSourceFactory} for creating {@link RandomSource}.
 */
public class RandomSourceFactory implements TableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		return Collections.singletonMap(
			ConnectorDescriptorValidator.CONNECTOR_TYPE, RandomSourceValidator.CONNECTOR_TYPE_VALUE);
	}

	@Override
	public List<String> supportedProperties() {
		return Arrays.asList(
			RandomSourceValidator.RANDOM_LIMIT,
			Schema.SCHEMA + ".#." + DescriptorProperties.DATA_TYPE,
			Schema.SCHEMA + ".#." + DescriptorProperties.NAME
		);
	}

	@Override
	public TableSource<Row> createTableSource(Map<String, String> propertyMap) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(propertyMap);

		TableSchema schema = properties.getTableSchema(Schema.SCHEMA);
		Optional<Integer> limit = properties.getOptionalInt(RandomSourceValidator.RANDOM_LIMIT);
		return new RandomSource(schema, limit.orElse(RandomSourceValidator.RANDOM_LIMIT_DEFAULT_VALUE));
	}
}
