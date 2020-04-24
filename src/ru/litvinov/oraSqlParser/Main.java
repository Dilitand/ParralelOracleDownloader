package ru.litvinov.oraSqlParser;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import ru.litvinov.oraSqlParser.ThreadWork.ThreadDaemonWatcherWorker;
import ru.litvinov.oraSqlParser.ThreadWork.ThreadSaver;
import ru.litvinov.oraSqlParser.fileWorker.FileWorker;
import ru.litvinov.oraSqlParser.oraWorker.OraWorkerImpl;
import ru.litvinov.oraSqlParser.ThreadWork.ThreadWorker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static java.lang.System.exit;

public class Main {

    private static String path = "output.txt";
    private static Integer countThreads = 5;
    private static Integer pieceSize = 500;

    private static String loggingProperties = "handlers = java.util.logging.FileHandler, java.util.logging.ConsoleHandler\n" +
            "\n" +
            "java.util.logging.FileHandler.level = INFO\n" +
            //"java.util.logging.FileHandler.pattern = log%u%g.txt\n" +
            "java.util.logging.FileHandler.limit = 10000000\n" +
            "java.util.logging.FileHandler.count = 5\n" +
            "java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter\n" +
            "\n" +
            "java.util.logging.ConsoleHandler.level = INFO\n" +
            "java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter";

    //После первой иницииализации логгер юзается просто вот так:
    private static Logger log = Logger.getLogger(Main.class.getName());

    static {
        try {
            //File file = new File(".\\resources\\logging.properties");
            //FileInputStream fileInputStream = new FileInputStream(file);
            //Чтение из файла
            //LogManager.getLogManager().readConfiguration(fileInputStream);
            //Чтение из стринга
            LogManager.getLogManager().readConfiguration(new ByteInputStream(loggingProperties.getBytes(),loggingProperties.length()) /*fileInputStream*/);
            log.addHandler(new FileHandler("log.txt"));
        } catch (IOException e) {
            e.printStackTrace();
            exit(0);
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        log.info("Programm started, count of threads: " + (args.length == 0 ? countThreads : args[0]));
        //Можно в параметрах поменять количество потоков
        if (args.length > 0){
            countThreads = new Integer(args[0]);
            pieceSize = new Integer(args[1]) > 0 && new Integer(args[1]) < 500 ? new Integer(args[1]) : 500;
        }

        OraWorkerImpl oraWorker = new OraWorkerImpl(log);
        try {
            oraWorker.getConnection();
        } catch (SQLException e) {
            log.info(e.toString());
            exit(0);
        }

        log.info("Parsing IDs.txt");
        List<List<String>> lists = oraWorker.readArraysId("IDs.txt",pieceSize);//Получаем коллекции c ID по 500 штук, больше уже глючит

        log.info("Starting threadpools");
        oraWorker.setExecutorService(Executors.newFixedThreadPool(countThreads));//этот сервис будет параллельно в 5 потоков выгружать
        oraWorker.setSheduledService(Executors.newScheduledThreadPool(1));//этот сервис будет последовательно сохранять

        for (List<String> list : lists) {
            String sqlStatement = oraWorker.makeSql(list);
            oraWorker.futures.add(oraWorker.getExecutorService().submit(new ThreadWorker(oraWorker, sqlStatement)));
        }

        log.info("Starting threadDaemonWatcher");
        //Демон следит за тем что все потоки выполнились и вырубает шедулер.
        Thread threadDaemonWatcher = new Thread(new ThreadDaemonWatcherWorker(oraWorker));
        threadDaemonWatcher.setDaemon(true);
        threadDaemonWatcher.start();

        log.info("Starting threadSaver");
        //Следит что поток завершен сохраняет результат и удаляет
        Thread threadSaver = new Thread(new ThreadSaver(oraWorker,lists,path));
        threadSaver.start();

        //singleThreadRun();
    }

    public static String getHeaders(ResultSetMetaData resultSetMetaData) throws SQLException {
        String columns = "";
        for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
            columns += resultSetMetaData.getColumnName(i) + ";";
        }
        return columns;
    }

    private static class Downloader implements Runnable {
        String[] ids;
        Downloader(String[] ids) {
            this.ids = ids;
        }

        @Override
        public void run() {

        }
    }
}
