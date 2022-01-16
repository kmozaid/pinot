package org.apache.pinot.common.utils.helix;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.config.UserConfigUtils;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UserCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserCache.class);
    private static final String USER_CONFIG_PARENT_PATH = "/CONFIGS/USER";
    private static final String USER_CONFIG_PATH_PREFIX = "/CONFIGS/USER/";

    private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

    private final UserConfigChangeListener _userConfigChangeListener = new UserConfigChangeListener();

    private final Map<String, UserConfig> _userConfigMap = new ConcurrentHashMap<>();
    private final Map<String, UserConfig> _userControllerConfigMap = new ConcurrentHashMap<>();
    private final Map<String, UserConfig> _userBrokerConfigMap = new ConcurrentHashMap<>();

    public UserCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
        _propertyStore = propertyStore;
        synchronized (_userConfigChangeListener) {
            _propertyStore.subscribeChildChanges(USER_CONFIG_PARENT_PATH, _userConfigChangeListener);

            List<String> users = _propertyStore.getChildNames(USER_CONFIG_PARENT_PATH, AccessOption.PERSISTENT);
            if (CollectionUtils.isNotEmpty(users)) {
                List<String> pathsToAdd = new ArrayList<>(users.size());
                for (String user : users) {
                    pathsToAdd.add(USER_CONFIG_PATH_PREFIX + user);
                }
                addUserConfigs(pathsToAdd);
            }
        }
    }

    @Nullable
    public List<UserConfig> getAllUserConfig() {
        return _userConfigMap.values().stream().collect(Collectors.toList());
    }

    public List<UserConfig> getAllControllerUserConfig() {
        return _userControllerConfigMap.values().stream().collect(Collectors.toList());
    }

    public List<UserConfig> getAllBrokerUserConfig() {
        return _userBrokerConfigMap.values().stream().collect(Collectors.toList());
    }


    @Nullable
    public List<String> getAllUserName() {
        return _userConfigMap.keySet().stream().collect(Collectors.toList());
    }

    private void addUserConfigs(List<String> paths) {
        for (String path : paths) {
            _propertyStore.subscribeDataChanges(path, _userConfigChangeListener);
        }
        List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT, false);
        for (ZNRecord znRecord : znRecords) {
            if (znRecord != null) {
                try {
                    UserConfig userConfig = UserConfigUtils.fromZNRecord(znRecord);
//                    String username = userConfig.getUserName();
                    String username = userConfig.getUsernameWithComponent();

                    if (userConfig.getComponentType().equals(ComponentType.CONTROLLER)) {
                        _userControllerConfigMap.put(username, userConfig);
                    } else if (userConfig.getComponentType().equals(ComponentType.BROKER)) {
                        _userBrokerConfigMap.put(username, userConfig);
                    }
//                    String usernameWithComponentType = userConfig.getUsernameWithComponent();
//                    _userConfigMap.put(username, userConfig);
                } catch (Exception e) {
                    LOGGER.error("Caught exception while adding user config for ZNRecord:{}", znRecord.getId(), e);
                }
            }
        }
    }

    private void removeUserConfig(String path) {
        _propertyStore.unsubscribeDataChanges(path, _userConfigChangeListener);
        String username = path.substring(USER_CONFIG_PATH_PREFIX.length());
        if (username.endsWith("BROKER")) {
            _userBrokerConfigMap.remove(username);
        } else if (username.endsWith("CONTROLLER")) {
            _userControllerConfigMap.remove(username);
        }
//        _userConfigMap.remove(username);
    }



    private class UserConfigChangeListener implements IZkChildListener, IZkDataListener {


        @Override
        public void handleChildChange(String path, List<String> userList) throws Exception {
            if (CollectionUtils.isEmpty(userList)) {
                return;
            }

            List<String> pathsToAdd = new ArrayList<>();
            for (String user : userList) {
                if (!_userControllerConfigMap.containsKey(user) && !_userBrokerConfigMap.containsKey(user)) {
                    pathsToAdd.add(USER_CONFIG_PATH_PREFIX + user);
                }
            }
            if (!pathsToAdd.isEmpty()) {
                addUserConfigs(pathsToAdd);
            }
        }

        @Override
        public void handleDataChange(String path, Object data) throws Exception {
            if (data != null) {
                ZNRecord znRecord = (ZNRecord) data;
                try {
                    UserConfig userConfig = UserConfigUtils.fromZNRecord(znRecord);
                    String usernameWithComponentType = userConfig.getUsernameWithComponent();
                    if (userConfig.getComponentType().equals(ComponentType.CONTROLLER)) {
                        _userControllerConfigMap.put(usernameWithComponentType, userConfig);
                    } else if (userConfig.getComponentType().equals(ComponentType.BROKER)) {
                        _userBrokerConfigMap.put(usernameWithComponentType, userConfig);
                    }
//                    _userConfigMap.put(userConfig.getUserName(), userConfig);
                } catch (Exception e) {
                    LOGGER.error("caught exception while refreshing table config for ZNRecord: {}", znRecord.getId(), e);
                }
            }
        }

        @Override
        public void handleDataDeleted(String path) throws Exception {
            String username = path.substring(path.lastIndexOf('/') + 1);
            removeUserConfig(USER_CONFIG_PATH_PREFIX + username);
        }
    }

}
