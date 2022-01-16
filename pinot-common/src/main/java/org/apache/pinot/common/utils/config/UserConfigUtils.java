package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.utils.EncryptUtils;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.UserConfig;

public class UserConfigUtils {
    private UserConfigUtils() {
    }

    public static UserConfig fromZNRecord(ZNRecord znRecord) {
        Map<String, String> simpleFields = znRecord.getSimpleFields();

        // Mandatory fields
        String username = simpleFields.get(UserConfig.USERNAME_KEY);
        String password = EncryptUtils.decrypt(simpleFields.get(UserConfig.PASSWORD_KEY));
        String component = simpleFields.get(UserConfig.COMPONET_KEY);
        String role = simpleFields.get(UserConfig.ROLE_KEY);

        List<String> tableList = znRecord.getListField(UserConfig.TABLES_KEY);

        List<String> permissionListFromZNRecord = znRecord.getListField(UserConfig.PERMISSIONS_KEY);
        List<AccessType> permissionList = null;
        if (permissionListFromZNRecord != null) {
            permissionList = permissionListFromZNRecord.stream().map(x -> AccessType.valueOf(x)).collect(Collectors.toList());
        }
        return new UserConfig(username, password, component, role, tableList, permissionList);
    }

    public static ZNRecord toZNRecord(UserConfig userConfig)
            throws JsonProcessingException {
        Map<String, String> simpleFields = new HashMap<>();

        // Mandatory fields
        simpleFields.put(UserConfig.USERNAME_KEY, userConfig.getUserName());
        simpleFields.put(UserConfig.PASSWORD_KEY, EncryptUtils.encrypt(userConfig.getPassword()));
        simpleFields.put(UserConfig.COMPONET_KEY, userConfig.getComponentType().toString());
        simpleFields.put(UserConfig.ROLE_KEY, userConfig.getRoleType().toString());

        Map<String, List<String>> listFields = new HashMap<>();


        // Optional fields
        List<String> tableList = userConfig.getTables();
        if (tableList != null) {
            listFields.put(UserConfig.TABLES_KEY, userConfig.getTables());
        }

        List<AccessType> permissionList = userConfig.getPermissios();
        if (permissionList != null) {
            listFields.put(UserConfig.PERMISSIONS_KEY, userConfig.getPermissios().stream().map(e -> e.toString()).collect(Collectors.toList()));
        }


        ZNRecord znRecord = new ZNRecord(userConfig.getUserName());
        znRecord.setSimpleFields(simpleFields);
        znRecord.setListFields(listFields);

        return znRecord;
    }


}
