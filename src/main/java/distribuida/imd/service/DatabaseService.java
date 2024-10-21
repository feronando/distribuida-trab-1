package distribuida.imd.service;

import distribuida.imd.domain.Conta;

import java.sql.*;

public class DatabaseService {
    private Connection connection;
    private static final String URL = "jdbc:mysql://localhost:3306/banco_distribuida";
    private static final String USER = "root";
    private static final String PASSWORD = "rootP@ss123";

    public DatabaseService() throws SQLException {
        connection = DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public Conta autenticarConta(String usuario, String senha) {
        String sql = "select * from contas where (usuario = ? and senha = ?)";
        try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
            sttmt.setString(1, usuario);
            sttmt.setString(2, senha);
            ResultSet resultSet = sttmt.executeQuery();

            if (resultSet.next()) {
                return new Conta(usuario, resultSet.getDouble("saldo"));
            }
        } catch (SQLException e) {
            System.out.println("Erro ao autenticar a conta: " + e.getMessage());
        }
        return null;
    }

    private boolean contaExiste(String usuario) {
        String sql = "select 1 from contas where usuario = ?";
        try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
            sttmt.setString(1, usuario);
            ResultSet resultSet = sttmt.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            System.out.println("Erro ao verificar se a conta existe: " + e.getMessage());
        }
        return false;
    }

    public boolean criarConta(String usuario, String senha) {
        String sql = "insert into contas (usuario, senha, saldo) values (?, ?, 0)";
        try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
            sttmt.setString(1, usuario);
            sttmt.setString(2, senha);
            sttmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("Erro ao criar a conta: " + e.getMessage());
            return false;
        }
    }

    public boolean removerConta(String usuario, String senha) {
        Conta conta = autenticarConta(usuario, senha);
        if (conta != null) {
            try{
                String sql1 = "DELETE FROM transacoes WHERE usuario = ?";
                try (PreparedStatement sttmtTransacoes = connection.prepareStatement(sql1)) {
                    sttmtTransacoes.setString(1, usuario);
                    sttmtTransacoes.executeUpdate();
                }

                String sql2 = "delete from contas where usuario = ?";
                try (PreparedStatement sttmt = connection.prepareStatement(sql2)) {
                    sttmt.setString(1, usuario);
                    sttmt.executeUpdate();
                    return true;
                }
            } catch (SQLException e) {
                System.out.println("Erro ao remover a conta: " + e.getMessage());
            }
        }
        return false;
    }

    public boolean depositar(String usuario, String senha, double valor) {
        Conta conta = autenticarConta(usuario, senha);
        if (conta != null || (contaExiste(usuario) && senha.equals("root"))) {
            String sql = "update contas set saldo = saldo + ? where usuario = ?";
            try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
                sttmt.setDouble(1, valor);
                sttmt.setString(2, usuario);
                sttmt.executeUpdate();

                registrarTransacao(usuario, "Depósito", valor);
                return true;
            } catch (SQLException e) {
                System.out.println("Erro ao depositar na conta: " + e.getMessage());
            }
        }
        return false;
    }

    public boolean sacar(String usuario, String senha, double valor) {
        Conta conta = autenticarConta(usuario, senha);
        if (conta != null) {
            String sql = "update contas set saldo = saldo - ? where (usuario = ? and saldo >= ?)";
            try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
                sttmt.setDouble(1, valor);
                sttmt.setString(2, usuario);
                sttmt.setDouble(3, valor);
                int aux = sttmt.executeUpdate();

                if (aux > 0) {
                    registrarTransacao(usuario, "Saque", valor);
                    return true;
                }
            } catch (SQLException e) {
                System.out.println("Erro ao sacar da conta: " + e.getMessage());
            }
        }
        return false;
    }

    public boolean transferir(String usuarioOrigem, String senhaOrigem, String usuarioDestino, double valor) {
        try {
            connection.setAutoCommit(false);

            if (sacar(usuarioOrigem, senhaOrigem, valor) && depositar(usuarioDestino, "root", valor)) {
                registrarTransacao(usuarioOrigem, "Transferência enviada para " + usuarioDestino, valor);
                registrarTransacao(usuarioDestino, "Transferência recebida de " + usuarioOrigem, valor);
                connection.commit();
                return true;
            }
            connection.rollback();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("Erro ao reverter a transação: " + ex.getMessage());
            }
            System.out.println("Erro ao transferir: " + e.getMessage());
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Erro ao restaurar o modo de commit automático: " + e.getMessage());
            }
        }
        return false;
    }

    private void registrarTransacao(String usuario, String descricao, double valor) {
        try {
            String sql = "insert into transacoes (usuario, descricao, valor) values (?, ?, ?)";
            PreparedStatement sttmt = connection.prepareStatement(sql);
            sttmt.setString(1, usuario);
            sttmt.setString(2, descricao);
            sttmt.setDouble(3, valor);
            sttmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Erro ao registrar a transação: " + e.getMessage());
        }
    }

    public ResultSet retirarExtrato(String usuario, String senha) {
        Conta conta = autenticarConta(usuario, senha);
        if (conta != null) {
            String sql = "select * from transacoes where usuario = ? order by data desc";
            try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
                sttmt.setString(1, usuario);
                return sttmt.executeQuery();
            } catch (SQLException e) {
                System.out.println("Erro ao retirar extrato: " + e.getMessage());
            }
        }
        return null;
    }

    public double retirarSaldo(String usuario, String senha) {
        Conta conta = autenticarConta(usuario, senha);
        if (conta != null) {
            String sql = "select saldo from contas where usuario = ?";
            try {
                PreparedStatement sttmt = connection.prepareStatement(sql);
                sttmt.setString(1, usuario);
                ResultSet resultSet = sttmt.executeQuery();

                if (resultSet.next()) {
                    return resultSet.getDouble("saldo");
                }
            } catch (SQLException e) {
                System.out.println("Erro ao retirar saldo: " + e.getMessage());
            }
        }
        return -1;
    }
}
