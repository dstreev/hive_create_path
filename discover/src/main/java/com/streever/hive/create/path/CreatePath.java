package com.streever.hive.create.path;

//import org.apache.hadoop.fs.StorageType;

import com.cloudera.utils.hadoop.hms.util.StorageType;

import static com.cloudera.utils.hadoop.hms.util.StorageType.*;

public enum CreatePath {

    CREATE_01(Boolean.FALSE, null),
    CREATE_02( Boolean.TRUE, null),
    CREATE_03(Boolean.FALSE, ORC),
    CREATE_04(Boolean.FALSE, PARQUET),
    CREATE_05(Boolean.FALSE, TEXTFILE),
    CREATE_06(Boolean.TRUE, ORC),
    CREATE_07(Boolean.TRUE, PARQUET),
    CREATE_08(Boolean.TRUE, TEXTFILE)
    ;
    private StorageType storageType;
    private Boolean external;

    CreatePath(Boolean external, StorageType storageType) {
        this.storageType = storageType;
        this.external = external;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public Boolean getExternal() {
        return external;
    }

    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        if (external) {
            sb.append("CREATE EXTERNAL TABLE");
        } else {
            sb.append("CREATE TABLE");
        }
//        sb.append("->");
//        if (fileType == null) {
//            sb.append("default");
//        } else {
//            sb.append(fileType);
//        }
        return sb.toString();
    }
    public String getCreateSql() {
        StringBuilder sb = new StringBuilder();
        if (!external) {
            sb.append("CREATE TABLE ");
        } else {
            sb.append("CREATE EXTERNAL TABLE ");
        }
        sb.append("TEST_").append(this.ordinal());
        sb.append(" ( id string ) ");
        if (storageType != null) {
            sb.append("STORED AS ").append(storageType.toString());
        }
        return sb.toString();
    }

    public String showCreateSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW CREATE TABLE ").append("TEST_").append(this.ordinal());
        return sb.toString();
    }
}
