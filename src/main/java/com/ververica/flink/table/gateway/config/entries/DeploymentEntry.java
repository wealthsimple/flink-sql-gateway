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

package com.ververica.flink.table.gateway.config.entries;

import com.ververica.flink.table.gateway.config.ConfigUtil;
import com.ververica.flink.table.gateway.config.Environment;

import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.flink.table.gateway.config.Environment.DEPLOYMENT_ENTRY;

/**
 * Configuration of a Flink cluster deployment. This class parses the `deployment` part
 * in an environment file. In the future, we should keep the amount of properties here little and
 * forward most properties to Flink's CLI frontend properties directly.
 *
 * <p>All properties of this entry are optional and evaluated lazily.
 */
public class DeploymentEntry extends ConfigEntry {

	private static final Logger LOG = LoggerFactory.getLogger(DeploymentEntry.class);

	public static final DeploymentEntry DEFAULT_INSTANCE =
		new DeploymentEntry(new DescriptorProperties(true));

	private static final String DEPLOYMENT_RESPONSE_TIMEOUT = "response-timeout";

	private static final String DEPLOYMENT_GATEWAY_ADDRESS = "gateway-address";

	private static final String DEPLOYMENT_GATEWAY_PORT = "gateway-port";

	private static final String DEPLOYMENT_DYNAMIC_FLINK_CONF = "dynamic-flink-conf";

	private DeploymentEntry(DescriptorProperties properties) {
		super(properties);
	}

	@Override
	protected void validate(DescriptorProperties properties) {
		properties.validateLong(DEPLOYMENT_RESPONSE_TIMEOUT, true, 0);
		properties.validateString(DEPLOYMENT_GATEWAY_ADDRESS, true, 0);
		properties.validateInt(DEPLOYMENT_GATEWAY_PORT, true, 0, 65535);
	}

	public long getResponseTimeout() {
		return properties.getOptionalLong(DEPLOYMENT_RESPONSE_TIMEOUT)
			.orElseGet(() -> useDefaultValue(DEPLOYMENT_RESPONSE_TIMEOUT, 10000L));
	}

	public String getGatewayAddress() {
		return properties.getOptionalString(DEPLOYMENT_GATEWAY_ADDRESS)
			.orElseGet(() -> useDefaultValue(DEPLOYMENT_GATEWAY_ADDRESS, ""));
	}

	public int getGatewayPort() {
		return properties.getOptionalInt(DEPLOYMENT_GATEWAY_PORT)
			.orElseGet(() -> useDefaultValue(DEPLOYMENT_GATEWAY_PORT, 0));
	}

	public Map<String, String> getDynamicFlinkConf() {
		String dynamicFlinkConfStr = properties.getOptionalString(DEPLOYMENT_DYNAMIC_FLINK_CONF).orElse("");
		Map<String, String> dynamicFlinkConfMap = Maps.newHashMap();

		String[] conf = dynamicFlinkConfStr.split(";");
		Arrays.stream(conf).forEach(kvStr -> {
				String[] kv = kvStr.split("=");
					if (kv.length == 2 && !"".equals(kv[0].trim()) && !"".equals(kv[1].trim())) {
						dynamicFlinkConfMap.put(kv[0], kv[1]);
					}
				}
		);
		return dynamicFlinkConfMap;
	}

	/**
	 * Parses the given command line options from the deployment properties. Ignores properties
	 * that are not defined by options.
	 */
	public CommandLine getCommandLine(Options commandLineOptions) throws Exception {
		final List<String> args = new ArrayList<>();

		properties.asMap().forEach((k, v) -> {
			// only add supported options
			if (commandLineOptions.hasOption(k)) {
				final Option o = commandLineOptions.getOption(k);
				final String argument = "--" + o.getLongOpt();
				// options without args
				if (!o.hasArg()) {
					final boolean flag = Boolean.parseBoolean(v);
					// add key only
					if (flag) {
						args.add(argument);
					}
				}
				// add key and value
				else if (!o.hasArgs()) {
					args.add(argument);
					args.add(v);
				}
				// options with multiple args are not supported yet
				else {
					throw new IllegalArgumentException("Option '" + o + "' is not supported yet.");
				}
			}
		});
		return CliFrontendParser.parse(commandLineOptions, args.toArray(new String[0]), true);
	}

	private <V> V useDefaultValue(String key, V defaultValue) {
		LOG.info("Property '{}.{}' not specified. Using default value: {}", DEPLOYMENT_ENTRY, key, defaultValue);
		return defaultValue;
	}

	public Map<String, String> asTopLevelMap() {
		return properties.asPrefixedMap(DEPLOYMENT_ENTRY + '.');
	}

	// --------------------------------------------------------------------------------------------

	public static DeploymentEntry create(Map<String, Object> config) {
		return new DeploymentEntry(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two deployments entries. The properties of the first deployment entry might be
	 * overwritten by the second one.
	 */
	public static DeploymentEntry merge(DeploymentEntry deployment1, DeploymentEntry deployment2) {
		final Map<String, String> mergedProperties = new HashMap<>(deployment1.asMap());
		mergedProperties.putAll(deployment2.asMap());

		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(mergedProperties);

		return new DeploymentEntry(properties);
	}

	/**
	 * Creates a new deployment entry enriched with additional properties that are prefixed with
	 * {@link Environment#DEPLOYMENT_ENTRY}.
	 */
	public static DeploymentEntry enrich(DeploymentEntry deployment, Map<String, String> prefixedProperties) {
		final Map<String, String> enrichedProperties = new HashMap<>(deployment.asMap());

		prefixedProperties.forEach((k, v) -> {
			final String normalizedKey = k.toLowerCase();
			if (k.startsWith(DEPLOYMENT_ENTRY + '.')) {
				enrichedProperties.put(normalizedKey.substring(DEPLOYMENT_ENTRY.length() + 1), v);
			}
		});

		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(enrichedProperties);

		return new DeploymentEntry(properties);
	}

	@Override
	public String toString() {
		return "DeploymentEntry{" + "properties=" + properties + '}';
	}
}
