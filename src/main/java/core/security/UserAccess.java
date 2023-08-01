package core.security;

public interface UserAccess {

    String getUserId();

    boolean checkAccess(String catalog, String tableName, String columnName);
}
