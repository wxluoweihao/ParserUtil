package core.security;

import core.security.UserAccess;

import java.util.HashMap;
import java.util.Map;

public class FakeUserAccess implements UserAccess {
    private String userId;
    private Map<String, Boolean> accessMap;

    public FakeUserAccess(String userId) {
        this.userId = userId;
        this.accessMap = new HashMap<>();
    }

    public UserAccess addAccess(String accessStr) {
        if (accessStr != "") {
            String[] split = accessStr.split(",");
            for(String access : split) {
                accessMap.put(access.toLowerCase(), true);
            }
        }

        return this;
    }

    public UserAccess upsertAccess(String accessStr, boolean allowAccess) {
        if (accessStr != "") {
            String[] split = accessStr.split(",");
            for(String access : split) {
                accessMap.put(access.toLowerCase(), allowAccess);
            }
        }

        return this;
    }

    @Override
    public String getUserId() {
        return this.userId;
    }

    @Override
    public boolean checkAccess(String catalog, String tableName, String columnName) {
        if(this.accessMap.containsKey(catalog + "." + tableName + "." + columnName)) {
            return true;
        }
        else {
            return false;
        }
    }
}
