package distribuida.imd.service;

import distribuida.imd.domain.ClientReplyAddress;

import java.io.*;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteAheadLog {
    private final String logFilePath;
    private final Map<String, String> taskStatus;
    private final Map<String, Long> taskTimestamps;
    private final Map<String, ClientReplyAddress> taskReplyAddress;

    public WriteAheadLog(String logFilePath) {
        this.logFilePath = logFilePath;
        this.taskStatus = new HashMap<>();
        this.taskTimestamps = new HashMap<>();
        this.taskReplyAddress = new HashMap<>();
    }

    public void log(String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true))) {
            writer.write(message + "\n");
            taskTimestamps.put(message.split(";")[0], Instant.now().toEpochMilli());
        } catch (IOException e) {
            System.out.println("Erro ao escrever no arquivo de log: " + e.getMessage());
        }
    }

    public void updateTaskStatus(String taskId, String status) {
        taskStatus.put(taskId, status);
    }

    public void updateTaskReplyAddress(String taskId, InetAddress address, Integer port) {
        taskReplyAddress.put(taskId, new ClientReplyAddress(address, port));
    }

    public String getTaskStatus(String taskId) {
        return taskStatus.get(taskId);
    }

    public Long getTaskTimestamp(String taskId) { return taskTimestamps.get(taskId); }

    public ClientReplyAddress getTaskReplyAddress(String taskId) { return taskReplyAddress.get(taskId); }

    public List<String> readPendingRequests() {
        List<String> pendingRequests = new ArrayList<>();
        File logFile = new File(logFilePath);
        if (logFile.exists() && logFile.length() != 0) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String request;
                while ((request = reader.readLine()) != null) {
                    String[] tokens = request.split(";");
                    if (tokens.length >= 4 && getTaskStatus(tokens[0]).equals("Pendente")) {
                        pendingRequests.add(request);
                    }
                }
            } catch (IOException e) {
                System.out.println("Erro ao ler do arquivo de log: " + e.getMessage());
            }
        }
        return pendingRequests;
    }

    public void clearLogFile() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, false));
        } catch (IOException e) {
            System.out.println("Erro ao limpar o arquivo de log: " + e.getMessage());
        }
    }
}
