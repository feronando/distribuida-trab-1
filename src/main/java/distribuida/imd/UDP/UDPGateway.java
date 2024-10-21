package distribuida.imd.UDP;

import distribuida.imd.domain.Gateway;
import distribuida.imd.service.WriteAheadLog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;


public class UDPGateway extends Gateway {
    private ConcurrentHashMap<Integer, Long> heartbeatTimestamps;
    private ScheduledThreadPoolExecutor threadScheduler;

    public UDPGateway()  {
        super();
        heartbeatTimestamps = new ConcurrentHashMap<>();
        threadScheduler = new ScheduledThreadPoolExecutor(3);
    }

    public void iniciar() {
        try {
            DatagramSocket gatewaySocket = new DatagramSocket(gatewayPort);
            System.out.println("Gateway UDP Iniciado");

            WriteAheadLog logger = new WriteAheadLog("requestsUDP.log");
            logger.clearLogFile();

            threadScheduler.schedule(this::registrarServidor, 0, TimeUnit.MILLISECONDS);
            threadScheduler.scheduleWithFixedDelay(this::validarHeartbeat, 1000, 1000, TimeUnit.MILLISECONDS);
            threadScheduler.scheduleWithFixedDelay(() -> processarRequisicoesPendentes(gatewaySocket, logger), 5000, 5000, TimeUnit.MILLISECONDS);

            while (true) {
                byte[] receiveData = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                gatewaySocket.receive(receivePacket);

                synchronized (liveServers){
                    if (liveServers.isEmpty()) {
                        System.out.println("Erro - Nenhum servidor disponível.");
                        Thread.sleep(1000);
                        continue;
                    }
                    int currServerIndex = currServer.updateAndGet(value -> (value + 1) % liveServers.size());
                    Integer currServerPort = liveServers.get(currServerIndex);

                    executorService.submit(() -> tratarRequisicao(gatewaySocket, receivePacket, currServerPort, logger));
                }
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Gateway UDP encerrando");
        } finally {
            executorService.shutdownNow();
            threadScheduler.shutdownNow();

        }
    }

    private void tratarRequisicao(DatagramSocket gatewaySocket, DatagramPacket receivePacket, Integer serverPort, WriteAheadLog logger) {
        try {
            String message = new String(receivePacket.getData()).trim();

            if (isClientRequest(message)) {
                String randomID = UUID.randomUUID().toString();
                String messageWithId = randomID + ";" + message;

                logger.log(messageWithId);
                logger.updateTaskStatus(randomID, "Pendente");
                logger.updateTaskReplyAddress(randomID, receivePacket.getAddress(), receivePacket.getPort());
                System.out.println("Enviando requisição com ID: " + randomID);

                DatagramPacket sendPacket = new DatagramPacket(
                        messageWithId.getBytes(), messageWithId.length(),
                        InetAddress.getByName("localhost"), serverPort);
                gatewaySocket.send(sendPacket);
            } else if (message.contains("ACK")) {
                try {
                    String[] tokens = message.split(";");
                    if (tokens.length == 4 && tokens[1].equals("ACK")) {
                        String requestID = tokens[0];
                        String statusRequest = tokens[2];

                        logger.updateTaskStatus(requestID, statusRequest);

                        String reply;
                        if (statusRequest.equals("OK")) {
                            reply = "Operação completada com sucesso para a requisição ID: " + requestID + "\nResultado do servidor: " + tokens[3];
                        } else {
                            reply = "Operação falhou para a requisição ID: " + requestID + "\nResultado do servidor: " + tokens[3];
                        }
                        System.out.println(reply);

                        DatagramPacket sendPacket = new DatagramPacket(
                                reply.getBytes(),reply.length(),
                                logger.getTaskReplyAddress(requestID).address(),
                                logger.getTaskReplyAddress(requestID).port());
                        gatewaySocket.send(sendPacket);
                    } else {
                        System.out.println("Formato de resposta inválido do servidor.");
                    }
                } catch (Exception e) {
                    System.out.println("Erro ao processar resposta do servidor: " + e.getMessage());
                }
            } else {
                System.out.println("Erro ao processar requisição " + message);
                System.out.println("A operação ou formato de requisição é inválido");
            }
        } catch (IOException e) {
            System.out.println("Erro ao comunicar com o servidor na porta " + serverPort);
        }
    }

    private void registrarServidor() {
        try {
            DatagramSocket heartbeatSocket = new DatagramSocket(heartbeatPort);
            System.out.println("Servidor Heartbeat Iniciado");

            while (true) {
                byte[] receiveData = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                try {
                    heartbeatSocket.receive(receivePacket);
                    String response = new String(receivePacket.getData()).trim();
                    if (response.equals("HEARTBEAT")) {
                        Boolean servidorLigado = liveServers.addIfAbsent(receivePacket.getPort());
                        if (servidorLigado) {
                            System.out.println("Servidor ligado na porta " + receivePacket.getPort());
                        }
                        heartbeatTimestamps.put(receivePacket.getPort(), Instant.now().toEpochMilli());
                    }
                } catch (IOException ex) {
                    continue;
                }
            }
        } catch (Exception e){
            System.out.println("Servidor Heartbeat encerrando");
        }
    }

    private void validarHeartbeat() {
        List<Integer> serversToRemove = new ArrayList<>();
        for (Integer serverPort : liveServers) {
            Long lastHeartbeatTime = heartbeatTimestamps.get(serverPort);

            if (lastHeartbeatTime == null || Instant.now().toEpochMilli() - lastHeartbeatTime > 5000) {
                serversToRemove.add(serverPort);
            }
        }

        synchronized (liveServers) {
            for (Integer serverPort : serversToRemove) {
                liveServers.remove(serverPort);
                heartbeatTimestamps.remove(serverPort);
                System.out.println("Servidor na porta " + serverPort + " morreu");
            }

            currServer.updateAndGet(value -> Math.max(value - serversToRemove.size(), -1));
        }
    }

    private void processarRequisicoesPendentes(DatagramSocket gatewaySocket, WriteAheadLog logger) {
        long currentTime = Instant.now().toEpochMilli();
        List<String> pendingRequests = logger.readPendingRequests();

        for (String request : pendingRequests) {
            String taskId = request.split(";")[0];

            if (currentTime - logger.getTaskTimestamp(taskId) > 5000) {
                System.out.println("Reenviando requisição pendente ID: " + taskId);
                try {
                    int currServerIndex = currServer.getAndUpdate(value -> (value + 1) % liveServers.size());
                    DatagramPacket resendPacket = new DatagramPacket(
                            request.getBytes(), request.length(),
                            InetAddress.getByName("localhost"), liveServers.get(currServer.get()));
                    gatewaySocket.send(resendPacket);
                } catch (IOException e) {
                    System.out.println("Erro ao reenviar a requisição ID: " + taskId + " - " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        UDPGateway server = new UDPGateway();
        new Thread(server::iniciar).start();
    }
}
