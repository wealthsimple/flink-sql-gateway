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

package com.ververica.flink.table.gateway.context;

import com.ververica.flink.table.gateway.config.Environment;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Context describing current session properties, original properties, ExecutionContext, etc.
 */
public class SessionContext {
	private final String sessionName;
	private final String sessionId;
	private final Environment originalSessionEnv;
	private final DefaultContext defaultContext;
	private ExecutionContext<?> executionContext;

	public SessionContext(
			@Nullable String sessionName,
			String sessionId,
			Environment originalSessionEnv,
			DefaultContext defaultContext) {
		this.sessionName = sessionName;
		this.sessionId = sessionId;
		this.originalSessionEnv = originalSessionEnv;
		this.defaultContext = defaultContext;
		this.executionContext = createExecutionContextBuilder(originalSessionEnv).build();
	}

	public Optional<String> getSessionName() {
		return Optional.ofNullable(sessionName);
	}

	public String getSessionId() {
		return this.sessionId;
	}

	public Environment getOriginalSessionEnv() {
		return this.originalSessionEnv;
	}

	public ExecutionContext<?> getExecutionContext() {
		return executionContext;
	}

	public void setExecutionContext(ExecutionContext<?> executionContext) {
		this.executionContext = executionContext;
	}

	/** Returns ExecutionContext.Builder with given {@link SessionContext} session context. */
	public ExecutionContext.Builder createExecutionContextBuilder(Environment sessionEnv) {
		return ExecutionContext.builder(
			defaultContext.getDefaultEnv(),
			sessionEnv,
			defaultContext.getDependencies(),
			defaultContext.getFlinkConfig(),
			defaultContext.getClusterClientServiceLoader(),
			defaultContext.getCommandLineOptions(),
			defaultContext.getCommandLines());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SessionContext)) {
			return false;
		}
		SessionContext context = (SessionContext) o;
		return Objects.equals(sessionName, context.sessionName) &&
			Objects.equals(sessionId, context.sessionId) &&
			Objects.equals(originalSessionEnv, context.originalSessionEnv) &&
			Objects.equals(executionContext, context.executionContext);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sessionName, sessionId, originalSessionEnv, executionContext);
	}
}
