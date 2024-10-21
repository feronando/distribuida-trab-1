package distribuida.imd.UDP;

import distribuida.imd.service.DatabaseService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.InetAddress;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;

public class UDPServer {
    private final Integer gatewayRegistrationPort = 9007;
    private final Integer[] serverPorts = {9001, 9002, 9003, 9004, 9005};
    private Integer serverPort;
    private DatagramSocket serverSocket;
    public ExecutorService executorService;
    private ScheduledThreadPoolExecutor threadScheduler;
    private final DatabaseService database;

    public UDPServer() throws SQLException {
        this.serverPort = -1;
        this.serverSocket = null;
        this.database = new DatabaseService();
        this.threadScheduler = new ScheduledThreadPoolExecutor(1);
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    private Boolean connectToNextPortAvailable() {
        for (int port : serverPorts) {
            if (isPortAvailable(port)) {
                try {
                    this.serverSocket = new DatagramSocket(port);
                    this.serverPort = port;
                    System.out.println("Servidor UDP iniciado na porta " + port);
                    return true;
                } catch (IOException e) {
                    System.out.println("Erro ao iniciar o servidor na porta " + port);
                }
            }
        }
        return false;
    }

    private boolean isPortAvailable(int port) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public void iniciar() {
        if (!connectToNextPortAvailable() || serverSocket == null) {
            System.out.println("Erro - Nenhuma porta disponível encontrada para iniciar o servidor.");
            exit(-1);
        }
        threadScheduler.scheduleWithFixedDelay(() -> registrarGateway(), 0, 1000, TimeUnit.MILLISECONDS);
        try {
            while (true) {
                byte[] receiveData = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                executorService.submit(()->tratarRequisicao(receivePacket));
            }
        } catch (Exception e) {
            System.out.println("Erro no servidor: " + e.getMessage());
        } finally {
            desligar();
        }
    }

    private void desligar() {
        if (serverSocket != null) {
            serverSocket.close();
        }
        executorService.shutdownNow();
        System.out.println("Servidor UDP encerrando");
    }

    private void tratarRequisicao(DatagramPacket receivePacket) {
        String message = new String(receivePacket.getData()).trim();
        System.out.println("Requisição recebida: " + message);
        String responseMessage = processarOperacao(message);
        DatagramPacket responsePacket = new DatagramPacket(
                responseMessage.getBytes(), responseMessage.length(),
                receivePacket.getAddress(), receivePacket.getPort());
        try {
            serverSocket.send(responsePacket);
        } catch (Exception e) {
            System.out.println("Erro ao processar requisição: " + e.getMessage());
        }
    }

    private String processarOperacao(String message) {
        if (message.equals("HEARTBEAT")) {
            return "ALIVE";
        } else {
            final String statusRequestOK = "OK;";
            final String statusRequestFail = "Falhou;";

            String[] tokens = message.split(";");
            String responseMessage = tokens[0] + ";ACK;";

            if (tokens.length < 4) {
                return responseMessage + statusRequestFail + "Formato de requisição inválido";
            }

            String operacao = tokens[1];
            String usuario = tokens[2];
            String senha = tokens[3];

            switch (operacao.toUpperCase()) {
                case "CRIAR":
                    if (database.criarConta(usuario, senha)) {
                        responseMessage += statusRequestOK + "Conta criada com o usuário: " + usuario;
                    } else {
                        responseMessage += statusRequestFail + "Conta já existe";
                    }
                    break;

                case "REMOVER":
                    if (database.removerConta(usuario, senha)) {
                        responseMessage += statusRequestOK + "Conta com o usuário " + usuario + " removida.";
                    } else {
                        responseMessage += statusRequestFail + "Remoção falhou ou conta não encontrada";
                    }
                    break;

                case "SALDO":
                    double saldo = database.retirarSaldo(usuario, senha);
                    if (saldo >= 0) {
                        responseMessage += statusRequestOK + "Saldo da conta com o usuário " + usuario + ": " + saldo;
                    } else {
                        responseMessage += statusRequestFail + "Conta não encontrada ou senha incorreta";
                    }
                    break;

                case "EXTRATO":
                    try {
                        ResultSet transacoes = database.retirarExtrato(usuario, senha);
                        if (transacoes != null) {
                            StringBuilder sttmtBuilder = new StringBuilder("Extrato da conta com usuário " + usuario + ":\n");
                            while (transacoes.next()) {
                                sttmtBuilder.append(transacoes.getString("descricao"))
                                        .append(" - ")
                                        .append(transacoes.getDouble("valor"))
                                        .append("\n");
                            }
                            responseMessage = statusRequestOK + sttmtBuilder.toString();
                        } else {
                            responseMessage = statusRequestFail + " Conta não encontrada ou senha incorreta";
                        }
                    } catch (SQLException e) {
                        responseMessage = statusRequestFail + " Erro ao recuperar extrato: " + e.getMessage();
                    } catch (Exception e) {
                        responseMessage = statusRequestFail + " Ocorreu um erro inesperado: " + e.getMessage();
                    }
                    break;

                case "DEPOSITAR":
                    if (tokens.length != 5) {
                        responseMessage += statusRequestFail + "Formato de requisição inválido";
                        break;
                    }
                    double valorDeposito = Double.parseDouble(tokens[4]);
                    if (database.depositar(usuario, senha, valorDeposito)) {
                        responseMessage += statusRequestOK + "Depositado " + valorDeposito + " na conta com o usuário: " + usuario;
                    } else {
                        responseMessage += statusRequestFail + "Depósito falhou";
                    }
                    break;

                case "SACAR":
                    if (tokens.length != 5) {
                        responseMessage += statusRequestFail + "Formato de requisição inválido";
                        break;
                    }
                    double valorSaque = Double.parseDouble(tokens[4]);
                    if (database.sacar(usuario, senha, valorSaque)) {
                        responseMessage += statusRequestOK + "Sacou " + valorSaque + " da conta com o usuário: " + usuario;
                    } else {
                        responseMessage += statusRequestFail + "Saque falhou";
                    }
                    break;

                case "TRANSFERIR":
                    if (tokens.length != 6) {
                        responseMessage += statusRequestFail + "Formato de requisição inválido";
                        break;
                    }
                    String usuarioDestino = tokens[4];
                    double valorTransferencia = Double.parseDouble(tokens[5]);
                    if (database.transferir(usuario, senha, usuarioDestino, valorTransferencia)) {
                        responseMessage += statusRequestOK + "Transferido " + valorTransferencia + " de " + usuario + " para " + usuarioDestino;
                    } else {
                        responseMessage += statusRequestFail + "Transferência falhou";
                    }
                    break;

                default:
                    responseMessage += statusRequestFail + "Operação inválida";
                    break;
            }
            return responseMessage;
        }
    }

    private void registrarGateway() {
        try {
            String heartbeatMessage = "HEARTBEAT";
            byte[] sendData = heartbeatMessage.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName("localhost"), gatewayRegistrationPort);
            serverSocket.send(sendPacket);
        } catch (IOException e) {
            System.out.println("Erro ao enviar heartbeat: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        try {
            UDPServer server = new UDPServer();
            new Thread(server::iniciar).start();
        } catch (Exception e) {
            System.out.println("Servidor falhou: " + e.getMessage());
        }

//        String responseMessage = "oi mundo";
//
//        for (int i = 0; i < 5; i++) {
//            try {
//                DatagramSocket serverSocket = new DatagramSocket();
//                byte[] responseData = (responseMessage + String.valueOf(i)).getBytes();
//                DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length, InetAddress.getByName("localhost"), 9001);
//                serverSocket.send(responsePacket);
//                Thread.sleep(5000);
//            } catch (Exception e) {
//                System.out.println("Error sending response: " + e.getMessage());
//            }
//
//        }
    }
}

