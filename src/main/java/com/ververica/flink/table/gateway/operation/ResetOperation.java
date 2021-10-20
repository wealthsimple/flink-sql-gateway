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

package com.ververica.flink.table.gateway.operation;

import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.context.ExecutionContext;
import com.ververica.flink.table.gateway.context.SessionContext;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

/**
 * Operation for RESET command.
 */
public class ResetOperation implements NonJobOperation {
	private final SessionContext context;
	private final String key;

	public ResetOperation(SessionContext context) {
		this(context, null);
	}

	public ResetOperation(SessionContext context, String key) {
		this.context = context;
		this.key = key;
	}

	@Override
	public ResultSet execute() {
		ExecutionContext<?> executionContext = context.getExecutionContext();
		Environment env = executionContext.getEnvironment();
		// reset all properties
		if (key == null) {
			// Renew the ExecutionContext by merging the default environment with original session context.
			// Book keep all the session states of current ExecutionContext then
			// re-register them into the new one.
			ExecutionContext<?> newExecutionContext = context
					.createExecutionContextBuilder(context.getOriginalSessionEnv())
					.sessionState(executionContext.getSessionState())
					.build();
			context.setExecutionContext(newExecutionContext);

			return OperationUtil.OK;
		} else {
			// reset a property
			Environment newEnv = Environment.enrich(env, ImmutableMap.of(key.trim(), null), ImmutableMap.of());
			ExecutionContext.SessionState sessionState = executionContext.getSessionState();

			// Renew the ExecutionContext by new environment.
			// Book keep all the session states of current ExecutionContext then
			// re-register them into the new one.
			ExecutionContext<?> newExecutionContext = context
					.createExecutionContextBuilder(context.getOriginalSessionEnv())
					.env(newEnv)
					.sessionState(sessionState)
					.build();
			context.setExecutionContext(newExecutionContext);

			return OperationUtil.OK;
		}

	}
}
