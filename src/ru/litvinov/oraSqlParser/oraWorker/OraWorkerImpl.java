package ru.litvinov.oraSqlParser.oraWorker;

import ru.litvinov.oraSqlParser.fileWorker.FileWorker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class OraWorkerImpl implements OraWorker {

    private String paramsPath = "ConnectionParameters.txt";
    private String sqlPath = "SQL.txt";
    private Map<String, String> params;
    public FileWorker fileWorker = new FileWorker();
    private List<String[]> listArrayId = new ArrayList<>();
    private List<String> listId = new ArrayList<>();
    public volatile String headers;
    private Logger logger;

    public List<Future<String>> futures = new ArrayList<>();//new CopyOnWriteArrayList<>();//Collections.synchronizedList(new ArrayList<>());
    private ExecutorService executorService;
    private ScheduledExecutorService sheduledService;

    public OraWorkerImpl() {
        readParams();
    }

    public OraWorkerImpl(String paramsPath)  {
        this.paramsPath = paramsPath;
        readParams();
    }

    public OraWorkerImpl(Logger logger)  {
        this.logger = logger;
        readParams();
    }

    public void readParams() {
        HashMap<String, String> hashMap = new HashMap<>();
        String p = fileWorker.readFile(paramsPath);
        String[] strings = p.split(";");
        for (String s : strings) {
            hashMap.put(s.split("=")[0], s.split("=")[1]);
        }
        if (!Stream.of("host", "port", "sid", "user", "pwr").allMatch(x -> hashMap.containsKey(x))) {
            throw new RuntimeException("Некорректный файл с параметрами");
        }
        params = hashMap;
    }

    @Override
    public Connection getConnection() throws SQLException {
        OraConnector.setLog(logger);
        OraConnector oraConnector = OraConnector.getInstance(params);
        return oraConnector.getConnection();
    }

    public Logger getLogger() {
        return logger;
    }

    public void disconnect(){
        OraConnector.disconnect();
    }

    @Override
    public List<String> readId(String path) {
        listId = Arrays.asList(fileWorker.readFile("IDs.txt").split("\n"));
        return new ArrayList<>(listId);
    }

    //Разбивает id шки по 500 штук в пачке, больше не вариант как показала практика - зависает resultSet.
    public List<List<String>> readArraysId(String path,Integer pieceSize) throws FileNotFoundException {
        List<String> list = readId(path);
        List<List<String>> result = new ArrayList<>();
        int counter = -1;
        for (int i = 0; i < list.size(); i++) {
            if (i % pieceSize == 0) {
                counter++;
                result.add(new ArrayList<>());
            }
            result.get(counter).add(list.get(i));
        }
        return result;
    }

    public String makeSql(List<String> ids)  {
        String insertId = "";
        for (String s : ids) {
            insertId += "'" + s.trim() + "',";
        }
        insertId = "(" + insertId.substring(0, insertId.length() - 1) + ")";
        return fileWorker.readFile(sqlPath).replaceAll("~ID~", insertId);
    }

    public void writeResults(String path, String output) throws IOException {
        fileWorker.writeFile(path, output);
    }

    public void setHeaders(ResultSetMetaData resultSetMetaData) throws SQLException {
        String columns = headers;
        if (columns == null) {
            synchronized (this) {
                columns = headers;
                if (columns == null) {
                    columns = "";
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        columns += resultSetMetaData.getColumnName(i) + ";";
                    }
                }
            }
        }
        headers = columns;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public ScheduledExecutorService getSheduledService() {
        return sheduledService;
    }

    public void setSheduledService(ScheduledExecutorService sheduledService) {
        this.sheduledService = sheduledService;
    }

    public String getHeaders() {
        return headers;
    }

    public Map<String, String> getParams() {
        return params;
    }
}
