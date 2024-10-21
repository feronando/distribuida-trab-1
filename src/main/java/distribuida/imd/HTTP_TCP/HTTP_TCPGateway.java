package distribuida.imd.HTTP_TCP;

import distribuida.imd.domain.Gateway;

import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.*;

import static java.lang.System.exit;

public class HTTP_TCPGateway extends Gateway {
    private final String protocolo;
    private ConcurrentHashMap<Integer, Long> heartbeatTimestamps;
    private ScheduledThreadPoolExecutor threadScheduler;

    public HTTP_TCPGateway(String protocolo) {
        super();
        this.gatewayPort = 9050;
        heartbeatTimestamps = new ConcurrentHashMap<>();
        threadScheduler = new ScheduledThreadPoolExecutor(2);
        if (!protocolo.toUpperCase().equals("TCP") && !protocolo.toUpperCase().equals("HTTP")) {
            System.err.println("Erro - Protocolo fornecido inválido");
            exit(-1);
        }

        this.protocolo = protocolo;
    }

    public void iniciar() {
        ServerSocket gatewaySocket = null;

        try {
            gatewaySocket = new ServerSocket(gatewayPort);
            System.out.println("Gateway" + protocolo + "Iniciado");

            threadScheduler.schedule(() -> registrarServidor(), 0, TimeUnit.MILLISECONDS);
            threadScheduler.scheduleWithFixedDelay(this::validarHeartbeat, 1000, 1000, TimeUnit.MILLISECONDS);

            while (true) {
                Socket clientSocket = gatewaySocket.accept();

                synchronized (liveServers) {
                    if (liveServers.isEmpty()) {
                        System.out.println("Erro - Nenhum servidor disponível.");
                        clientSocket.close();
                        Thread.sleep(1000);
                        continue;
                    }
                    int currServerIndex = currServer.updateAndGet(value -> (value + 1) % liveServers.size());
                    Integer currServerPort = liveServers.get(currServerIndex);

                    executorService.submit(() -> tratarRequisicao(clientSocket, currServerPort));
                }
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Gateway TCP encerrando");
        } finally {
            executorService.shutdownNow();
            threadScheduler.shutdownNow();
            try {
                if (gatewaySocket != null) {
                    gatewaySocket.close();
                }
            } catch (IOException e) {
                System.out.println("Erro ao fechar o gatewaySocket: " + e.getMessage());
            }
        }
    }

    private void tratarRequisicao(Socket clientSocket, Integer serverPort) {
        BufferedReader clientIn = null;
        PrintWriter clientOut = null;

        try {
            clientIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            clientOut = new PrintWriter(clientSocket.getOutputStream(), true);

            if(protocolo.equals("HTTP")) {
                processarRequisicaoHTTP(clientIn, clientOut, serverPort);
            } else {
                String message = clientIn.readLine().trim();
                System.out.println("RECEBEU REQUISIÇÂO: " + message);

                if (isClientRequest(message)) {
                    processarRequisicaoTCP(message, serverPort, clientOut);
                } else {
                    System.out.println("Erro ao processar requisição: " + message);
                    clientOut.println("Erro - Formato ou operação de requisição inválido.");
                }
            }
        } catch (IOException e) {
            System.out.println("Erro de comunicação com o cliente: " + e.getMessage());
        } finally {
            try {
                if (clientIn != null) {
                    clientIn.close();
                }
                if (clientOut != null) {
                    clientOut.close();
                }
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Erro ao fechar conexão com o cliente: " + e.getMessage());
            }
        }
    }

    private void processarRequisicaoTCP(String message, Integer serverPort, PrintWriter clientOut){
        String randomID = UUID.randomUUID().toString();
        String messageWithId = randomID + ";" + message;

        System.out.println("Enviando requisição com ID: " + randomID);

        Socket serverSocket = null;
        PrintWriter serverOut = null;
        BufferedReader serverIn = null;

        try {
            serverSocket = new Socket("localhost", serverPort);
            serverOut = new PrintWriter(serverSocket.getOutputStream(), true);
            serverIn = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));

            serverOut.println(messageWithId);
            String serverResponse = serverIn.readLine().trim();

            processarRespostaTCP(serverResponse, clientOut);
        } catch (IOException e) {
            System.out.println("Erro ao comunicar com o servidor na porta " + serverPort + ":" + e.getMessage());
            clientOut.println("Erro ao comunicar com o servidor.");
        } finally {
            try {
                if (serverIn != null) {
                    serverIn.close();
                }
                if (serverOut != null) {
                    serverOut.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Erro ao fechar conexão com o cliente: " + e.getMessage());
            }
        }
    }

    private void processarRespostaTCP(String serverResponse, PrintWriter clientOut){
        String[] tokens = serverResponse.split(";");

        if (tokens.length == 4 && tokens[1].equals("ACK")) {
            String requestID = tokens[0];
            String statusRequest = tokens[2];
            String responseBody = tokens[3];

            if (protocolo.equals("HTTP")) {
                processarRespostaHTTP(clientOut, statusRequest, responseBody);
            } else {
                if (statusRequest.equals("OK")) {
                    System.out.println("Operação completada com sucesso para a requisição ID: " + requestID);
                    clientOut.println("Operação finalizada com sucesso: " + tokens[3]);
                } else {
                    System.out.println("Operação falhou para a requisição ID: " + requestID);
                    clientOut.println("Sucesso no pipeline. Falha na operação. Resultado do servidor: " + tokens[3]);
                }
                System.out.println(tokens[3]);
            }
        } else {
            System.out.println("Formato de resposta inválido do servidor.");
            clientOut.println("Erro - Resposta inválida do servidor.");
        }
    }

    private void processarRequisicaoHTTP(BufferedReader clientIn, PrintWriter clientOut, Integer serverPort) throws IOException {
        String headerLine = clientIn.readLine();
        if (headerLine != null && !headerLine.isEmpty()) {
            StringTokenizer tokenizer = new StringTokenizer(headerLine);
            String httpMethod = tokenizer.nextToken();

            if (httpMethod.equals("GET") || httpMethod.equals("POST") || httpMethod.equals("PUT") || httpMethod.equals("DELETE")) {
                String httpQueryString = tokenizer.nextToken();
                System.out.println("RECEBEU REQUISIÇÂO HTTP: " + httpQueryString);
                processarRequisicaoTCP(httpQueryString, serverPort, clientOut);
            } else {
                System.out.println("O método HTTP não é reconhecido");
                processarRespostaHTTP(clientOut, "405", "Erro - Método HTTP não é reconhecido");
            }
        }
    }

    private void processarRespostaHTTP(PrintWriter clientOut, String statusRequest, String responseString) {
        String statusLine;
        String serverHeader = "Server: HTTP_TCPGateway\r\n";
        String contentTypeHeader = "Content-Type: text/html\r\n";
        String contentLengthHeader = "Content-Length: " + responseString.length() + "\r\n";

        if (statusRequest.equals("OK")) {
            statusLine = "HTTP/1.0 200 OK\r\n";
            System.out.println("Operação HTTP completada com sucesso");
            clientOut.write(statusLine);
            clientOut.write(serverHeader);
            clientOut.write(contentTypeHeader);
            clientOut.write(contentLengthHeader);
            clientOut.write("\r\n");
            clientOut.write("<html><body>Operação finalizada com sucesso: " + responseString + "</body></html>\r\n");
        } else {
            statusLine = "HTTP/1.0 500 Internal Server Error\r\n";
            System.out.println("Operação HTTP falhou");
            clientOut.write(statusLine);
            clientOut.write(serverHeader);
            clientOut.write(contentTypeHeader);
            clientOut.write(contentLengthHeader);
            clientOut.write("\r\n");
            clientOut.write("<html><body>sucesso no pipeline. Falha na operação. Resultado do servidor: " + responseString + "</body></html>\r\n");
        }
    }

    private void registrarServidor() {
        ServerSocket heartbeatSocket = null;
        try {
            heartbeatSocket = new ServerSocket(heartbeatPort);
            System.out.println("Servidor Heartbeat Iniciado");

            while (true) {
                    try {
                        Socket registerHeartbeatSocket = heartbeatSocket.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(registerHeartbeatSocket.getInputStream()));
                        String response = in.readLine().trim();
                        if (response.split(";")[0].equals("HEARTBEAT")) {
                            Integer serverPort = Integer.parseInt(response.split(";")[1]);
                            Boolean servidorLigado = liveServers.addIfAbsent(serverPort);
                            if (servidorLigado) {
                                System.out.println("Servidor ligado na porta " + serverPort);
                            }
                            heartbeatTimestamps.put(serverPort, Instant.now().toEpochMilli());
                        }
                    } catch (SocketTimeoutException e) {
                        continue;
                    }
            }
        } catch (IOException e) {
            System.out.println("Servidor Heartbeat encerrando");
        } finally {
            try {
                if (heartbeatSocket != null) {
                    heartbeatSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Erro ao fechar o servidor Heartbeat: " + e.getMessage());
            }
        }
    }

    private void validarHeartbeat() {
        List<Integer> serversToRemove = new ArrayList<>();
        for (Integer serverPort : liveServers) {
            Long lastHeartbeatTime = heartbeatTimestamps.get(serverPort);

            if (lastHeartbeatTime == null || Instant.now().toEpochMilli() - lastHeartbeatTime > 3000) {
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

    public static void main(String[] args) {
//        if (args.length != 1) {
//            System.err.println("Erro - Número inválido de argumentos fornecido");
//            exit(-1);
//        }
        HTTP_TCPGateway server = new HTTP_TCPGateway("TCP");
        new Thread(server::iniciar).start();
    }
}