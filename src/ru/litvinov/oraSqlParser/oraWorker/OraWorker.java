package ru.litvinov.oraSqlParser.oraWorker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface OraWorker {
    Connection getConnection() throws SQLException;
    void disconnect();
    List readId(String path) throws FileNotFoundException;

}
