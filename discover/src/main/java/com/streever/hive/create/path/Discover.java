package com.streever.hive.create.path;

import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.util.StorageType;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Discover {
    public static void main(String[] args) {
        // Establish Connection
        String driver_class = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://s03.streever.local:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/Users/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit";
        String username = "dstreev";
        String password = "judge-abnormal-precede";
        String db="z_create_path_validation";

        List<Boolean> booleans = new ArrayList<>(
                Arrays.asList(Boolean.TRUE, Boolean.FALSE)
        );
        Map<String, Boolean> sessionProps = new HashMap<String, Boolean>();

        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();
//        ObjectNode root = mapper.createObjectNode();
        ArrayNode arrayNode = mapper.createArrayNode();

        try {
            conn = DriverManager.getConnection(url, username, password);
            if (conn != null) {
                for (Boolean acid : booleans) {
                    sessionProps.put("hive.create.as.acid", acid);
                    for (Boolean insertOnly : booleans) {
                        sessionProps.put("hive.create.as.insert.only", insertOnly);
                        for (Boolean legacy : booleans) {
                            sessionProps.put("hive.create.as.external.legacy", legacy);
                            for (ExternalOnly eo : ExternalOnly.values()) {
                                for (DefaultTableType dtt : DefaultTableType.values()) {
                                    List<String> dbCreateList = createDb(db, dtt, eo);
                                    Statement statement = conn.createStatement();
                                    for (String sql: dbCreateList) {
                                        System.out.println(sql + ";");
                                        statement.execute(sql);
                                    }
                                    dbCreateList.stream().map(i -> i + ";").forEach(System.out::println);
                                    for (String key : sessionProps.keySet()) {
                                        System.out.println("set " + key + "=" + sessionProps.get(key).toString() + ";");
                                        statement.execute("set " + key + "=" + sessionProps.get(key).toString() );
                                    }
                                    // Cycle through the CreatePaths
                                    for (CreatePath cr : CreatePath.values()) {
                                        ObjectNode node = arrayNode.addObject();
                                        ObjectNode createNode = node.putObject("create");
                                        createNode.put("description", cr.getDescription());
                                        createNode.put("external", cr.getExternal());
                                        if (cr.getStorageType() == null) {
                                            createNode.put("storage_type", "default");
                                        } else {
                                            createNode.put("storage_type", cr.getStorageType().toString());
                                        }
//                                        node.put("created.storage_type",)
                                        ObjectNode envNode = node.putObject("environment");
                                        ObjectNode dbNode = envNode.putObject("db_properties");
                                        dbNode.put("defaultTableType", dtt.toString());
                                        dbNode.put("EXTERNAL_TABLES_ONLY", eo.toString());

                                        ObjectNode sessionNode = envNode.putObject("session_parameters");
                                        sessionNode.put("hive.create.as.acid", acid);
                                        sessionNode.put("hive.create.as.insert.only", insertOnly);
                                        sessionNode.put("hive.create.as.external.legacy", legacy);
                                        // Get Create Statement
                                        String cStatement = cr.getCreateSql();
                                        System.out.println(cStatement + ";");
                                        // Run Create Statement
                                        try {
                                            statement.execute(cStatement);
                                            String cShow = cr.showCreateSql();
                                            System.out.println(cShow + ";");
                                            // Run Show Create Statement
                                            ResultSet rSet = null;
                                            ObjectNode createdNode = node.putObject("created");

                                            try {
                                                rSet = statement.executeQuery(cShow);
                                                // This is the table definition.
                                                List<String> tableDefinition = getTableDefinition(rSet);
                                                // Need to determine table state.
                                            /*
                                            - Is it External
                                            - Has a Purge Flag
                                            - Is ACID
                                            - Is Insert Only Acid
                                            - Is it Translated
                                             */
                                                Boolean external = TableUtils.isExternal("temp", tableDefinition);
                                                createdNode.put("external", external);
                                                String transactional = TableUtils.getTblProperty(MirrorConf.TRANSACTIONAL, tableDefinition);
                                                if (transactional != null) {
                                                    createdNode.put(MirrorConf.TRANSACTIONAL, Boolean.valueOf(transactional));
                                                } else {
                                                    createdNode.put(MirrorConf.TRANSACTIONAL, Boolean.FALSE);
                                                }

                                                StorageType storageType = TableUtils.getStorageType(tableDefinition);
                                                if (storageType != null) {
                                                    createdNode.put("storage_type", storageType.toString());
                                                }

                                                String trans_property = TableUtils.getTblProperty(MirrorConf.TRANSACTIONAL_PROPERTIES, tableDefinition);
                                                if (trans_property != null) {
                                                    createdNode.put(MirrorConf.TRANSACTIONAL_PROPERTIES, trans_property);
                                                }
                                                String translated = TableUtils.getTblProperty("TRANSLATED_TO_EXTERNAL", tableDefinition);
                                                if (translated != null) {
                                                    createdNode.put("TRANSLATED_TO_EXTERNAL", Boolean.valueOf(translated));
                                                }
                                                String purge = TableUtils.getTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, tableDefinition);
                                                if (purge != null) {
                                                    createdNode.put(MirrorConf.EXTERNAL_TABLE_PURGE, Boolean.valueOf(purge));
                                                }
                                            } catch (SQLException sqlException) {
                                                // Process SQL Exception.
                                            } finally {
                                                if (rSet != null)
                                                    rSet.close();
                                            }
                                        } catch (SQLException createSqlException) {
                                            node.put("error", createSqlException.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException se) {
            se.printStackTrace();
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            System.out.print(arrayNode.toString());
        }

    }

    public static List<String> getTableDefinition(ResultSet resultSet) throws SQLException {
        List<String> tblDef = new ArrayList<String>();
        ResultSetMetaData meta = resultSet.getMetaData();
        if (meta.getColumnCount() >= 1) {
            while (resultSet.next()) {
                try {
                    tblDef.add(resultSet.getString(1).trim());
                } catch (NullPointerException npe) {
                    // catch and continue.
//                    LOG.error(getEnvironment() + ":" + database + "." + tableMirror.getName() +
//                            ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
//                            "ResultSet record(line) is null. Skipping.");
                }
            }
        } else {
//            LOG.error(getEnvironment() + ":" + database + "." + tableMirror.getName() +
//                    ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata.");
        }
        return tblDef;
    }
    public static List<String> createDb (String dbName, DefaultTableType defaultTableType, ExternalOnly externalOnly) {
        List<String> rtn = new ArrayList<String>();
        StringBuilder sbDrop = new StringBuilder();
        sbDrop.append("DROP DATABASE IF EXISTS ").append(dbName).append(" CASCADE");
        rtn.add(sbDrop.toString());
        StringBuilder sbCreate = new StringBuilder();
        sbCreate.append("CREATE DATABASE ").append(dbName).append(" ");
        Map<String, String> dbProperties = new HashMap<String, String>();
        if (defaultTableType != DefaultTableType.NA) {
            dbProperties.put("defaultTableType", defaultTableType.toString());
        }
        if (externalOnly != ExternalOnly.NA) {
            dbProperties.put("EXTERNAL_TABLES_ONLY", externalOnly.toString());
        }
        if (dbProperties.size() > 0) {
            sbCreate.append("WITH DBPROPERTIES (");
            Iterator<Map.Entry<String, String>> dbI = dbProperties.entrySet().iterator();
            while (dbI.hasNext()) {
              Map.Entry<String, String> item = dbI.next();
                sbCreate.append("\"").append(item.getKey()).append("\"=\"");
                sbCreate.append(item.getValue()).append("\"");
                if (dbI.hasNext()) {
                    sbCreate.append(", ");
                }
            }
            sbCreate.append(")");
        }
        rtn.add(sbCreate.toString());
        rtn.add("USE " + dbName);
        return rtn;
    }
}
