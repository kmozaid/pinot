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
package org.apache.pinot.controller.api.resources;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.message.ObjectMessage;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


/**
 * filter each Controller's restful API, record request info and username 2021-12-22
 *
 * @author Soldier
 */
@Singleton
@Provider
public class PinotControllerRequestAuditFilter implements ContainerResponseFilter {
  private static final org.apache.logging.log4j.Logger AUDIT_LOGGER = LogManager.getLogger("WAPAuditLog");

  private final static Marker MARKER_WHITESPACE = MarkerManager.getMarker("WAP_PINOT_AUDIT_LOG");
  private static final String INVALID_TOKEN = "Invalid Token";
  private static final String GET_METHOD = "GET";
  private static final String QUERY_FROM_UI = "sql";

  @Inject
  private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> _request;

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {

    if (StringUtils.equals(requestContext.getMethod(), GET_METHOD)) {
      return;
    }
    UriInfo uriInfo = requestContext.getUriInfo();

    //skip sql query from pinot controller UI
    if (StringUtils.equals(uriInfo.getPath(), QUERY_FROM_UI)) {
      return;
    }
    String srcIpAddr = _request.get().getRemoteAddr();
    String username = "";
    try {
      username = getUsernameFromToken(requestContext);
    } catch (Exception e) {
      username = INVALID_TOKEN + " " + e.toString();
    }
    Map<String, Object> map = new HashMap<>();
    map.put("srcIpAddr", srcIpAddr);
    map.put("username", username);
    map.put("method", requestContext.getMethod());
    map.put("api", uriInfo.getPath());
    map.put("url", uriInfo.getRequestUri().toString());
    map.put("responseCode", responseContext.getStatus());
    map.put("reasonPhrase", responseContext.getStatusInfo().getReasonPhrase());
    ObjectMessage msg = new ObjectMessage(map);
    AUDIT_LOGGER.info(MARKER_WHITESPACE, msg);
  }

  private String getUsernameFromToken(ContainerRequestContext requestContext)
      throws Exception {
    String username = "";
    String authorization = requestContext.getHeaderString("Authorization");
    if (authorization != null && authorization.split("\\s").length >= 2) {
      String token = authorization.split("\\s")[1];
      byte[] decodedBytes = Base64.getDecoder().decode(token);
      username = new String(decodedBytes);
    }
    return username;
  }
}
