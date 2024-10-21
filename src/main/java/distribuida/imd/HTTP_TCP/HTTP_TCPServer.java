package distribuida.imd.HTTP_TCP;

import distribuida.imd.service.DatabaseService;

import java.io.*;
import java.net.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;

public class HTTP_TCPServer {
    private final Integer gatewayRegistrationPort = 9007;
    private final Integer[] serverPorts = {9001, 9002, 9003, 9004, 9005};
    private Integer serverPort;
    private ServerSocket serverSocket;
    public ExecutorService executorService;
    private ScheduledThreadPoolExecutor threadScheduler;
    private final DatabaseService database;

    public HTTP_TCPServer() throws SQLException {
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
                    this.serverSocket = new ServerSocket(port);
                    this.serverPort = port;
                    System.out.println("Servidor TCP rodando na porta " + port);
                    return true;
                } catch (IOException e) {
                    System.out.println("Porta " + port + " não disponível.");
                }
            }
        }
        return false;
    }

    private boolean isPortAvailable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void iniciar() {
        if (!connectToNextPortAvailable() || serverSocket == null) {
            System.out.println("Erro - Nenhuma porta disponível encontrada para iniciar o servidor.");
            exit(-1);
        }
        threadScheduler.scheduleWithFixedDelay(this::registrarGateway, 0, 1000, TimeUnit.MILLISECONDS);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> processarRequisicao(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("Erro no servidor: " + e.getMessage());
        } finally {
            desligar();
        }
        System.out.println("AQUI");
    }

    private void desligar() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (Exception e) {
            System.out.println("Erro ao fechar conexão do servidor: " + e.getMessage());
        }
        executorService.shutdownNow();
        System.out.println("Servidor encerrando");
    }

    private void processarRequisicao(Socket clientSocket) {
        BufferedReader clientIn = null;
        PrintWriter clientOut = null;
        try {
            clientIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            clientOut = new PrintWriter(clientSocket.getOutputStream(), true);

            String request = clientIn.readLine().trim();
            System.out.println("Requisição recebida: " + request);

            String response = request.equals("HEARTBEAT") ? "ALIVE" : processarResposta(request);
            clientOut.println(response);
        } catch (IOException e) {
            System.out.println("Erro ao processar a requisição: " + e.getMessage());
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

    private String processarResposta(String message) {
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

    private void registrarGateway() {
        Socket heartbeatSocket = null;
        PrintWriter out = null;
        try {
            String heartbeatMessage = "HEARTBEAT;" + this.serverPort;
            heartbeatSocket = new Socket("localhost", gatewayRegistrationPort);
            out = new PrintWriter(heartbeatSocket.getOutputStream(), true);
            out.println(heartbeatMessage);
        } catch (IOException e) {
            System.out.println("Erro ao enviar heartbeat: " + e.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (heartbeatSocket != null) {
                    heartbeatSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Erro ao fechar conexão do heartbeat: " + e.getMessage());
            }
        }
    }
    public static void main(String[] args) throws SQLException {
        HTTP_TCPServer server = new HTTP_TCPServer();
        new Thread(server::iniciar).start();
    }
}
