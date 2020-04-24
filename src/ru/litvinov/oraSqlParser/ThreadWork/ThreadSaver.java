package ru.litvinov.oraSqlParser.ThreadWork;

import ru.litvinov.oraSqlParser.Main;
import ru.litvinov.oraSqlParser.oraWorker.OraWorkerImpl;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ThreadSaver implements Runnable {

    private OraWorkerImpl oraWorker;
    private List<List<String>> ids;
    private String path;
    private String logFile = LocalDateTime.now().getSecond() + "" +
            LocalDateTime.now().getMinute() + "" +
            LocalDateTime.now().getHour() + "_" +
            LocalDateTime.now().getDayOfMonth() + "" +
            LocalDateTime.now().getMonth() + "" + LocalDateTime.now().getYear();
    private boolean headersSaved;

    private Logger log;

    public ThreadSaver(OraWorkerImpl oraWorker, List<List<String>> ids, String path) {
        this.oraWorker = oraWorker;
        this.path = path;
        this.ids = ids;
        this.log = oraWorker.getLogger();
    }

    @Override
    public void run() {
        //��������� ������� ������� ����� ������ ����������� ������ ��������������� � ����
        while (true) {
            for (int i = 0; i < oraWorker.futures.size();i++)
            {
                Future future = oraWorker.futures.get(i);
                if (future.isDone()) {
                    oraWorker.getSheduledService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if (!headersSaved) {
                                    //����� �����
                                    oraWorker.fileWorker.writeFile(path, oraWorker.getHeaders());
                                    headersSaved = true;
                                }
                                oraWorker.fileWorker.writeFile(path, future.get().toString());
                            } catch (IOException | InterruptedException | ExecutionException e) {
                                log.severe("Exception " + Arrays.toString(e.getStackTrace()));
                                e.printStackTrace();
                            }
                        }
                    }, 500L, TimeUnit.MILLISECONDS);
                    //����� ������ ������� �� ��������� ����������� �������
                    oraWorker.futures.remove(i);
                    try {
                        if (future.get().getClass() != SQLSyntaxErrorException.class) {
                            //log.info("IDsDownloaded:\n " + ids.get(i).stream().reduce("", (x, y) -> x + "\n" + y));
                            //������� ����������� ����������� ID ������� ���������.
                            //oraWorker.fileWorker.writeFile("IsFinished" + logFile + ".txt", ids.get(i).stream().reduce("", (x, y) -> x + "\n" + y));
                        }
                        ids.remove(i);
                    } catch (ExecutionException | InterruptedException e) {
                        log.severe("Exception " + e.toString());
                        e.printStackTrace();
                    }
                }
            }
            if (oraWorker.futures.isEmpty()) {
                oraWorker.disconnect();
                oraWorker.getSheduledService().shutdown();
                log.info("Download completed");
                System.out.println("�������� ���������");
                break;
            }
        }
    }
}
