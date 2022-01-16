package org.apache.pinot.spi.config.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;



public class UserConfig extends BaseJsonConfig {
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String COMPONET_KEY = "component";
    public static final String ROLE_KEY = "role";
    public static final String AUTH_TOKEN_KEY = "authToken";
    public static final String TABLES_KEY = "tables";
    public static final String PERMISSIONS_KEY = "permissions";

    @JsonPropertyDescription("The name of User")
    private final String _username;

    @JsonPropertyDescription("The password of User")
    private final String _password;

    @JsonPropertyDescription("The name of Component")
    private final ComponentType _componentType;

    @JsonPropertyDescription("The role of user")
    private final RoleType _roleType;

    @JsonPropertyDescription("The tables owned of User")
    private final List<String> _tables;

    @JsonPropertyDescription("The table permission of User")
    private final List<AccessType> _permissions;

    @JsonCreator
    public UserConfig(@JsonProperty(value = USERNAME_KEY, required = true) String username,
                       @JsonProperty(value = PASSWORD_KEY, required = true) String password,
                      @JsonProperty(value = COMPONET_KEY, required = true) String component,
                      @JsonProperty(value = ROLE_KEY, required = true) String role,
                      @JsonProperty(value = TABLES_KEY) @Nullable List<String> tableList,
                      @JsonProperty(value = PERMISSIONS_KEY) @Nullable List<AccessType> permissionList
    ) {
        Preconditions.checkArgument(username != null, "'username' must be configured");
        Preconditions.checkArgument(password != null, "'password' must be configured");

        // NOTE: Handle lower case table type and raw table name for backward-compatibility
        _username = username;
        _password = password;
        _componentType = ComponentType.valueOf(component.toUpperCase());
        _roleType = RoleType.valueOf(role.toUpperCase());
        _tables = tableList;
        _permissions = permissionList;
    }

    @JsonProperty(USERNAME_KEY)
    public String getUserName() {
        return _username;
    }

    public String getUsernameWithComponent() {
        return getUserName() + "_" + getComponentType().toString();
    }

    public boolean isExist(String username, ComponentType component) {
        return _username.equals(username) && _componentType.equals(component);
    }

    @JsonProperty(PASSWORD_KEY)
    public String getPassword() {
        return _password;
    }

    public List<String> getTablesWithoutNull() {
        return Optional.ofNullable(_tables).orElseGet(() -> new ArrayList<>(Arrays.asList("")));
    }

    @JsonProperty(TABLES_KEY)
    public List<String> getTables() {
        return _tables;
    }


    @JsonProperty(PERMISSIONS_KEY)
    public List<AccessType> getPermissios() {
        return _permissions;
    }

    @JsonProperty(COMPONET_KEY)
    public ComponentType getComponentType() {
        return _componentType;
    }

    @JsonProperty(ROLE_KEY)
    public RoleType getRoleType() {
        return _roleType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        if (!super.equals(o)) {return false;}
        UserConfig that = (UserConfig) o;
        return _username.equals(that._username) && _componentType == that._componentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), _username, _componentType);
    }
}
