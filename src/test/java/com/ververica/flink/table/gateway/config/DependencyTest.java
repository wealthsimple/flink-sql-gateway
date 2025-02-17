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

package com.ververica.flink.table.gateway.config;

import com.ververica.flink.table.gateway.context.DefaultContext;
import com.ververica.flink.table.gateway.operation.SqlCommandParser.SqlCommand;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.gateway.rest.session.Session;
import com.ververica.flink.table.gateway.rest.session.SessionManager;
import com.ververica.flink.table.gateway.sink.TestTableSinkFactoryBase;
import com.ververica.flink.table.gateway.source.TestTableSourceFactoryBase;
import com.ververica.flink.table.gateway.utils.EnvironmentFileUtil;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactoryOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.CommonCatalogOptions.CATALOG_TYPE;
import static org.apache.flink.table.catalog.CommonCatalogOptions.DEFAULT_DATABASE_KEY;
import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.junit.Assert.assertEquals;

/**
 * Mainly for testing classloading of dependencies.
 *
 * <p>NOTE: before running this test, please make sure that {@code table-factories-test-jar.jar}
 * exists in the target directory. If not, run {@code mvn clean package} first.
 */
public class DependencyTest {

	public static final String CONNECTOR_TYPE_VALUE = "test-connector";
	public static final String TEST_PROPERTY = "test-property";

	public static final String CATALOG_TYPE_TEST = "DependencyTest";
	public static final String MODULE_TYPE_TEST = "ModuleDependencyTest";

	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-gateway-factory.yaml";
	private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";

	@Test
	public void testTableFactoryDiscovery() throws Exception {
		// create environment
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_CONNECTOR_TYPE", CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY", TEST_PROPERTY);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "test-value");
		final Environment env = EnvironmentFileUtil.parseModified(FACTORY_ENVIRONMENT_FILE, replaceVars);

		// create executor with dependencies
		final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
		DefaultContext defaultContext = new DefaultContext(
			env,
			Collections.singletonList(dependency),
			new Configuration(),
			new DefaultCLI(),
			new DefaultClusterClientServiceLoader());
		SessionManager sessionManager = new SessionManager(defaultContext);
		String sessionId = sessionManager.createSession("test", "blink", "streaming", Maps.newConcurrentMap());
		Session session = sessionManager.getSession(sessionId);
		Tuple2<ResultSet, SqlCommand> result = session.runStatement("DESCRIBE TableNumber1");
		assertEquals(SqlCommand.DESCRIBE_TABLE, result.f1);

		final List<Row> expectedData = Arrays.asList(
			Row.of("IntegerField1", "INT", true, null, null, null),
			Row.of("StringField1", "STRING", true, null, null, null),
			Row.of("rowtimeField", "TIMESTAMP(3) *ROWTIME*", true, null, null, null));
		ResultSet expected = ResultSet.builder()
			.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
			.columns(
				ColumnInfo.create(ConstantNames.DESCRIBE_NAME, DataTypes.STRING().getLogicalType()),
				ColumnInfo.create(ConstantNames.DESCRIBE_TYPE, DataTypes.STRING().getLogicalType()),
				ColumnInfo.create(ConstantNames.DESCRIBE_NULL, new BooleanType()),
				ColumnInfo.create(ConstantNames.DESCRIBE_KEY, DataTypes.STRING().getLogicalType()),
				ColumnInfo.create(ConstantNames.DESCRIBE_COMPUTED_COLUMN, DataTypes.STRING().getLogicalType()),
				ColumnInfo.create(ConstantNames.DESCRIBE_WATERMARK, DataTypes.STRING().getLogicalType()))
			.data(expectedData)
			.build();

		assertEquals(expected, result.f0);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Table source that can be discovered if classloading is correct.
	 */
	public static class TestTableSourceFactory extends TestTableSourceFactoryBase {

		public TestTableSourceFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * Table sink that can be discovered if classloading is correct.
	 */
	public static class TestTableSinkFactory extends TestTableSinkFactoryBase {

		public TestTableSinkFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * Module that can be discovered if classloading is correct.
	 */
	public static class TestModuleFactory implements ModuleFactory {

		@Override
		public Module createModule(Map<String, String> properties) {
			return new TestModule();
		}

		@Override
		public Map<String, String> requiredContext() {
			final Map<String, String> context = new HashMap<>();
			context.put(MODULE_TYPE, MODULE_TYPE_TEST);
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			final List<String> properties = new ArrayList<>();
			properties.add("test");
			return properties;
		}
	}

	/**
	 * Test module.
	 */
	public static class TestModule implements Module {

	}

	/**
	 * For backward compatibility for GenericInMemoryCatalogFactroy because [FLINK-21822].
	 */
	public static class TestGenericInMemoryCatalogFactory extends GenericInMemoryCatalogFactory {

		@Override
		public Map<String, String> requiredContext() {
			Map<String, String> context = new HashMap<>();
			context.put(CATALOG_TYPE.key(), GenericInMemoryCatalogFactoryOptions.IDENTIFIER); // generic_in_memory
			context.put(FactoryUtil.PROPERTY_VERSION.key(), "1"); // backwards compatibility
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			List<String> properties = new ArrayList<>();

			// default database
			properties.add(GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE.key());

			return properties;
		}

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

			final Optional<String> defaultDatabase = descriptorProperties.getOptionalString(GenericInMemoryCatalogFactoryOptions.DEFAULT_DATABASE.key());

			return new GenericInMemoryCatalog(name, defaultDatabase.orElse(GenericInMemoryCatalog.DEFAULT_DB));
		}

		private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
			final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
			descriptorProperties.putProperties(properties);

			// from 1.13 [FLINK-21822][table] Migrate built-in and test catalog factories to new stack
			// we just ignore validation
//			new GenericInMemoryCatalogValidator().validate(descriptorProperties);

			return descriptorProperties;
		}
	}

	/**
	 * Catalog that can be discovered if classloading is correct.
	 */
	public static class TestCatalogFactory implements CatalogFactory {

		@Override
		public Map<String, String> requiredContext() {
			final Map<String, String> context = new HashMap<>();
			context.put(CATALOG_TYPE.key(), CATALOG_TYPE_TEST);
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			final List<String> properties = new ArrayList<>();
			properties.add(DEFAULT_DATABASE_KEY);
			return properties;
		}

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			final DescriptorProperties params = new DescriptorProperties(true);
			params.putProperties(properties);

			final Optional<String> defaultDatabase = params.getOptionalString(DEFAULT_DATABASE_KEY);

			return new TestCatalog(name, defaultDatabase.orElse(GenericInMemoryCatalog.DEFAULT_DB));
		}
	}

	/**
	 * Test catalog.
	 */
	public static class TestCatalog extends GenericInMemoryCatalog {
		public TestCatalog(String name, String defaultDatabase) {
			super(name, defaultDatabase);
		}
	}

	/**
	 * A test factory that is the same as {@link HiveCatalogFactory}
	 * except returning a {@link HiveCatalog} always with an embedded Hive metastore
	 * to test logic of {@link HiveCatalogFactory}.
	 */
	public static class TestHiveCatalogFactory extends HiveCatalogFactory {
		public static final String ADDITIONAL_TEST_DATABASE = "additional_test_database";
		public static final String TEST_TABLE = "test_table";
		static final String TABLE_WITH_PARAMETERIZED_TYPES = "param_types_table";

		@Override
		public Map<String, String> requiredContext() {
			Map<String, String> context = new HashMap<>();
			context.put(CATALOG_TYPE.key(), "hive"); // hive
			context.put(FactoryUtil.PROPERTY_VERSION.key(), "1"); // backwards compatibility

			// For factory discovery service to distinguish TestHiveCatalogFactory from HiveCatalogFactory
			context.put("test", "test");
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			List<String> properties = new ArrayList<>();

			// default database
			properties.add(HiveCatalogFactoryOptions.DEFAULT_DATABASE.key());

			properties.add(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key());

			properties.add(HiveCatalogFactoryOptions.HIVE_VERSION.key());

			properties.add(HiveCatalogFactoryOptions.HADOOP_CONF_DIR.key());

			properties.add(CatalogPropertiesUtil.IS_GENERIC);

			return properties;
		}

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			// Developers may already have their own production/testing hive-site.xml set in their environment,
			// and Flink tests should avoid using those hive-site.xml.
			// Thus, explicitly create a testing HiveConf for unit tests here
			Catalog hiveCatalog = HiveTestUtils
				.createHiveCatalog(name, properties.get(HiveCatalogFactoryOptions.HIVE_VERSION));

			// Creates an additional database to test tableEnv.useDatabase() will switch current database of the catalog
			hiveCatalog.open();
			try {
				hiveCatalog.createDatabase(
					ADDITIONAL_TEST_DATABASE,
					new CatalogDatabaseImpl(new HashMap<>(), null),
					false);
				hiveCatalog.createTable(
					new ObjectPath(ADDITIONAL_TEST_DATABASE, TEST_TABLE),
					new CatalogTableImpl(
						TableSchema.builder()
							.field("testcol", DataTypes.INT())
							.build(),
						new HashMap<String, String>() {{
							put(CatalogPropertiesUtil.IS_GENERIC, String.valueOf(false));
						}},
						""
					),
					false
				);
				// create a table to test parameterized types
				hiveCatalog.createTable(new ObjectPath("default", TABLE_WITH_PARAMETERIZED_TYPES),
					tableWithParameterizedTypes(),
					false);
			} catch (DatabaseAlreadyExistException | TableAlreadyExistException | DatabaseNotExistException e) {
				throw new CatalogException(e);
			}

			return hiveCatalog;
		}

		private CatalogTable tableWithParameterizedTypes() {
			TableSchema tableSchema = TableSchema.builder().fields(new String[] { "dec", "ch", "vch" },
				new DataType[] { DataTypes.DECIMAL(10, 10), DataTypes.CHAR(5), DataTypes.VARCHAR(15) }).build();
			return new CatalogTableImpl(
				tableSchema,
				new HashMap<String, String>() {{
					put(CatalogPropertiesUtil.IS_GENERIC, String.valueOf(false));
				}},
				"");
		}
	}
}
