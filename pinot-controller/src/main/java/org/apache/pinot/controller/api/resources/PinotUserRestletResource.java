package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.UserAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.USER_TAG)
@Path("/")
public class PinotUserRestletResource {

    public static final Logger LOGGER = LoggerFactory.getLogger(PinotUserRestletResource.class);

    @Inject
    PinotHelixResourceManager _pinotHelixResourceManager;

    @Inject
    PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

    @Inject
    ControllerConf _controllerConf;

    @Inject
    ControllerMetrics _controllerMetrics;

    @Inject
    ExecutorService _executorService;

    @Inject
    AccessControlFactory _accessControlFactory;
    AccessControlUtils _accessControlUtils = new AccessControlUtils();

    @Inject
    Executor _executor;

    @Inject
    HttpConnectionManager _connectionManager;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/users")
    @ApiOperation(value = "Lists all uses in cluster", notes = "Lists all users in cluster")
    public String listUers(@Context HttpHeaders httpHeaders, @Context Request request) {
        try {
            String endpointUrl = request.getRequestURL().toString();
            _accessControlUtils
                    .validatePermission(httpHeaders, endpointUrl, _accessControlFactory.create());
            ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
            Map<String, List<String>> allUserInfo= ZKMetadataProvider.getAllUserInfo(propertyStore);
            return JsonUtils.newObjectNode().set("users", JsonUtils.objectToJsonNode(allUserInfo)).toString();
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
    }


    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/users")
    @ApiOperation(value = "Adds a user", notes = "Adds a user")
    public SuccessResponse addUser(String userConfigStr, @Context HttpHeaders httpHeaders, @Context Request request) {
        // TODO introduce a table config ctor with json string.

        UserConfig userConfig;
        String username;
        try {
            userConfig = JsonUtils.stringToObject(userConfigStr, UserConfig.class);

            username = userConfig.getUserName();
            String endpointUrl = request.getRequestURL().toString();

            _accessControlUtils
                    .validatePermission(username, httpHeaders, endpointUrl, _accessControlFactory.create());

            if (username.contains(".") || username.contains(" ")) {
                throw new IllegalStateException("Username: " + username + "' containing '.' or space is not allowed");
            }
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
        try {
            _pinotHelixResourceManager.addUser(userConfig);
            return new SuccessResponse(String.format("User %s has been successfully added!", userConfig.getUserName() + '_' + userConfig.getComponentType()));
        } catch (Exception e) {
            if (e instanceof UserAlreadyExistsException) {
                throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
            } else {
                throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);

            }
        }
    }

    @DELETE
    @Path("/users/{username}")
    @Authenticate(AccessType.DELETE)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Deletes a user", notes = "Deletes a user")
    public SuccessResponse deleteUser(@PathParam("username") String username, @QueryParam("component") String component, @Context HttpHeaders httpHeaders, @Context Request request) {

        List<String> usersDeleted = new LinkedList<>();
        String usernameWithComponentType = username + "_" + component;

        try {

            boolean userExist = false;
            userExist = _pinotHelixResourceManager.hasUser(username, component);

            String endpointUrl = request.getRequestURL().toString();
            _accessControlUtils.validatePermission(username, httpHeaders, endpointUrl, _accessControlFactory.create());

            _pinotHelixResourceManager.deleteUser(usernameWithComponentType);
            if (userExist) {
                usersDeleted.add(username);
            }
            if (!usersDeleted.isEmpty()) {
                return new SuccessResponse("User: " + usernameWithComponentType + " deleted");
            }
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }

        throw new ControllerApplicationException(LOGGER,
                "User " + usernameWithComponentType + " does not exists", Response.Status.NOT_FOUND);
    }


    @PUT
    @Path("/users/{username}")
    @Authenticate(AccessType.UPDATE)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
    public SuccessResponse updateUserConfig(
            @PathParam("username") String username,
            @QueryParam("component") String component,
            String userConfigString,
            @Context HttpHeaders httpHeaders,
            @Context Request request)
            throws Exception {

        UserConfig userConfig;
        String usernameWithComponentType = username + "_" + component;
        try {
            String endpointUrl = request.getRequestURL().toString();
            _accessControlUtils
                    .validatePermission(username, httpHeaders, endpointUrl, _accessControlFactory.create());

            userConfig = JsonUtils.stringToObject(userConfigString, UserConfig.class);
            String usernameWithComponentTypeFromUserConfig = userConfig.getUsernameWithComponent();
            if (!usernameWithComponentType.equals(usernameWithComponentTypeFromUserConfig)) {
                throw new ControllerApplicationException(LOGGER,
                        String.format("Request user %s does not match %s in the Request body", usernameWithComponentType, usernameWithComponentTypeFromUserConfig),
                        Response.Status.BAD_REQUEST);
            }
            if (!_pinotHelixResourceManager.hasUser(username, component)) {
                throw new ControllerApplicationException(LOGGER,
                        "Request user " + usernameWithComponentType + " does not exist",
                        Response.Status.NOT_FOUND);
            }
            _pinotHelixResourceManager.updateUserConfig(userConfig);

        } catch (Exception e) {
            throw e;
        }
        return new SuccessResponse("User config update for " + usernameWithComponentType);
    }
}
