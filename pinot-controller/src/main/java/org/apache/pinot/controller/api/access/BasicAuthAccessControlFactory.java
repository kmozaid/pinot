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
package org.apache.pinot.controller.api.access;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.helix.UserCache;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;


/**
 * Basic Authentication based on http headers. Configured via the "controller.admin.access.control" family of
 * properties.
 *
 * <pre>
 *     Example:
 *     controller.admin.access.control.principals=admin123,user456
 *     controller.admin.access.control.principals.admin123.password=verysecret
 *     controller.admin.access.control.principals.user456.password=kindasecret
 *     controller.admin.access.control.principals.user456.tables=stuff,lessImportantStuff
 *     controller.admin.access.control.principals.user456.permissions=read,update
 * </pre>
 */
public class BasicAuthAccessControlFactory implements AccessControlFactory {
  private static final String PREFIX = "controller.admin.access.control.principals";

  private static final String HEADER_AUTHORIZATION = "Authorization";

  private AccessControl _accessControl;

  @Override
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _accessControl = new BasicAuthAccessControl(new UserCache(propertyStore));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private Map<String, BasicAuthPrincipal> _token2principal;
    private final UserCache _userCache;

    public BasicAuthAccessControl(UserCache userCache) {
      _userCache = userCache;
    }

    @Override
    public boolean protectAnnotatedOnly() {
      return false;
    }

    @Override
    public boolean hasDataAccess(HttpHeaders httpHeaders, String tableName) {
      return getPrincipal(httpHeaders).filter(p -> p.hasTable(tableName)).isPresent();
    }

    @Override
    public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders)
          .filter(p -> p.hasTable(tableName) && p.hasPermission(Objects.toString(accessType))).isPresent();
    }

    @Override
    public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders).isPresent();
    }

    @Override
    public boolean hasAccess(HttpHeaders httpHeaders) {
      return getPrincipal(httpHeaders)
              .filter(p -> p.hasPermission(RoleType.ADMIN, ComponentType.CONTROLLER)).isPresent();
    }

    private Optional<BasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      if (headers == null) {
        return Optional.empty();
      }
      _token2principal = BasicAuthUtils.extractBasicAuthPrincipals(_userCache.getAllControllerUserConfig())
              .stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
      List<String> authHeaders = headers.getRequestHeader(HEADER_AUTHORIZATION);
      if (authHeaders == null) {
        return Optional.empty();
      }

      return authHeaders.stream().map(BasicAuthUtils::normalizeBase64Token).map(_token2principal::get)
              .filter(Objects::nonNull).findFirst();
    }


    @Override
    public AuthWorkflowInfo getAuthWorkflowInfo() {
      return new AuthWorkflowInfo(AccessControl.WORKFLOW_BASIC);
    }
  }
}
