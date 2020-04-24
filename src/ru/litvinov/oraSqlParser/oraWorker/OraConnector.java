package ru.litvinov.oraSqlParser.oraWorker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

public class OraConnector {

    private static Logger log;
    private static Map<String,String> params;
    private static volatile OraConnector instance;
    private static volatile Connection connection;

    private OraConnector() throws SQLException {
        connect();
    }

    public static OraConnector getInstance(Map map) {
        if (instance == null) {
            synchronized (OraConnector.class) {
                if (instance == null) {
                    params = map;
                    try {
                        instance = new OraConnector();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return instance;
    }

    public static void setLog(Logger logger) {
        if (log == null){
            synchronized (OraConnector.class) {
                if (log == null) {
                    OraConnector.log = logger;
                }
            }
        }
    }

    Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connect();
        }
        return connection;
    }

    public static void disconnect(){
        try {
            connection.close();
            log.info("disconnecting");
        } catch (SQLException e) {
            log.severe("Disco exception :" + e.toString());
        }
    }

    private void connect() {
        log.info("Connecting to server");
        try {
            String url = String.format("jdbc:oracle:thin:@%s:%s/%s", params.get("host"), params.get("port"), params.get("sid"));
            connection = DriverManager.getConnection(url, params.get("user"), params.get("pwr"));
        } catch (SQLException e) {
            log.severe("Connection is failed:" +  e.toString());
            e.printStackTrace();
            System.exit(0);
        }
        log.info("Connection established");
    }
}
