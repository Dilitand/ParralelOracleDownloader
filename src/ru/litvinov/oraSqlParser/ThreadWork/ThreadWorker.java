package ru.litvinov.oraSqlParser.ThreadWork;

import ru.litvinov.oraSqlParser.oraWorker.OraWorkerImpl;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class ThreadWorker implements Callable {
    private List<String> ids;
    private OraWorkerImpl oraWorker;
    private String sqlStatement;
    private StringBuilder result = new StringBuilder();

    private static Logger log = Logger.getLogger(ThreadWorker.class.getName());

    public ThreadWorker(OraWorkerImpl oraWorker, String sqlStatement) {
        this.sqlStatement = sqlStatement;
        this.oraWorker = oraWorker;
    }

    @Override
    public Object call() {

        try {
            Connection connection = oraWorker.getConnection();
            Statement statement = connection.createStatement();

            //Если схема ЕКП то запускаем экзекутор
            if (oraWorker.getParams().get("sid").toUpperCase().equals("EKP_REP")) {
                statement.execute("declare\n" +
                        "  i        number;\n" +
                        "begin\n" +
                        "  i := executor.lock_open;\n" +
                        "END;\n");
            }

            //Определяем кол-во столбцов
            ResultSet resultSet = statement.executeQuery(sqlStatement);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int countHeaders = resultSetMetaData.getColumnCount();
            //сохраняем шапку
            oraWorker.setHeaders(resultSetMetaData);
            resultSet = statement.executeQuery(sqlStatement);
            //System.out.println(sqlStatement);
            while (resultSet.next()) {
                StringBuilder stringBuilder = new StringBuilder("\n");
                //первый элемент начинается с единицы, мда...
                for (int j = 1; j <= countHeaders; j++) {
                    stringBuilder.append(resultSet.getString(j)).append(";");
                }
                result.append(stringBuilder);
            }
        }
        catch (SQLException e){
            log.severe("Exception " + Arrays.toString(e.getStackTrace()));
            return e;
        }
        //System.out.println(result);
        return result.toString();
    }
}
