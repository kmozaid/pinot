/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.auth;

import com.google.common.base.Preconditions;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Utility for configuring basic auth and parsing related http tokens
 */
public final class BasicAuthUtils {
  private static final String PASSWORD = "password";
  private static final String PERMISSIONS = "permissions";
  private static final String TABLES = "tables";
  private static final String ALL = "*";

  private BasicAuthUtils() {
    // left blank
  }

  /**
   * Parse a pinot configuration namespace for access control settings, e.g. "controller.admin.access.control
   * .principals".
   *
   * <pre>
   *     Example:
   *     my.prefix.access.control.principals=admin123,user456
   *     my.prefix.access.control.principals.admin123.password=verysecret
   *     my.prefix.access.control.principals.user456.password=kindasecret
   *     my.prefix.access.control.principals.user456.tables=stuff,lessImportantStuff
   *     my.prefix.access.control.principals.user456.permissions=read,update
   * </pre>
   *
   * @return list of BasicAuthPrincipals
   */

  public static List<BasicAuthPrincipal> extractBasicAuthPrincipals(List<UserConfig> userConfigList) {
    return userConfigList.stream()
            .map(user -> {
              String name = user.getUserName().trim();
              Preconditions.checkArgument(StringUtils.isNoneBlank(name), "%s is not a valid username", name);
              String password = user.getPassword().trim();
              Preconditions.checkArgument(StringUtils.isNoneBlank(password), "must provide a password for %s", name);
              String component = user.getComponentType().toString();
              String role = user.getRoleType().toString();

              Set<String> tables = Optional.ofNullable(user.getTables())
                      .orElseGet(() -> Collections.emptyList())
                      .stream().collect(Collectors.toSet());
              Set<String> permissions = Optional.ofNullable(user.getPermissios())
                      .orElseGet(() -> Collections.emptyList())
                      .stream().map(x -> x.toString())
                      .collect(Collectors.toSet());

              return new BasicAuthPrincipal(name, toBasicAuthToken(name, password), component, role, tables, permissions);
            }).collect(Collectors.toList());
  }


  private static Set<String> extractSet(PinotConfiguration configuration, String key) {
    String input = configuration.getProperty(key);
    if (StringUtils.isNotBlank(input) && !ALL.equals(input)) {
      return Arrays.stream(input.split(",")).map(String::trim).collect(Collectors.toSet());
    }
    return Collections.emptySet();
  }

  /**
   * Convert a pair of name and password into a http header-compliant base64 encoded token
   *
   * @param name user name
   * @param password password
   * @return base64 encoded basic auth token
   */
  @Nullable
  public static String toBasicAuthToken(String name, String password) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    String identifier = String.format("%s:%s", name, password);
    return normalizeBase64Token(String.format("Basic %s", Base64.getEncoder().encodeToString(identifier.getBytes())));
  }

  /**
   * Normalize a base64 encoded auth token by stripping redundant padding (spaces, '=')
   *
   * @param token base64 encoded auth token
   * @return normalized auth token
   */
  @Nullable
  public static String normalizeBase64Token(String token) {
    if (token == null) {
      return null;
    }
    return StringUtils.remove(token.trim(), '=');
  }
}
