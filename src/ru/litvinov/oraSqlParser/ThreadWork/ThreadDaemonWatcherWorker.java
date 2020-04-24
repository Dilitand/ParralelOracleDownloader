package ru.litvinov.oraSqlParser.ThreadWork;

import ru.litvinov.oraSqlParser.oraWorker.OraWorker;
import ru.litvinov.oraSqlParser.oraWorker.OraWorkerImpl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ThreadDaemonWatcherWorker implements Runnable {
    private List<Future<String>> futures;
    private ExecutorService executorService;

    public ThreadDaemonWatcherWorker(OraWorkerImpl oraWorker) {
        this.futures = oraWorker.futures;
        this.executorService = oraWorker.getExecutorService();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Integer optional = futures.stream().map(x -> x.isDone() ? 0 : 1).reduce(0, (x, y) -> x + y);
            if (optional == 0) {
                executorService.shutdown();
                break;
            }
            System.out.println(new String(LocalDateTime.now() + ", remaining of parts : ") + optional);
            Thread.yield();
        }
    }
}
