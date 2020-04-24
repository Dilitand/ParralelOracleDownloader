package ru.litvinov.oraSqlParser.fileWorker;

import java.io.*;

public class FileWorker {

    public synchronized String readFile(String path) {
        try (FileInputStream fileInputStream = new FileInputStream(path)) {
            byte[] b = new byte[fileInputStream.available()];
            fileInputStream.read(b);
            return new String(b, "Windows-1251");
        } catch (IOException f) {
            System.out.println("Ошибка при чтении файла: " + f.getMessage());
            //throw new FileNotFoundException("файл не найден");
        }
        return null;
    }

    public synchronized void writeFile(String path, String output) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(path, true)) {
            byte[] bytes = output.getBytes();
            fileOutputStream.write(bytes);
        }
    }
}
