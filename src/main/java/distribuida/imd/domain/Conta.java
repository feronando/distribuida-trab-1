package distribuida.imd.domain;

public class Conta {
    private final String usuario;
    private double saldo;

    public Conta(String usuario, double saldo) {
        this.usuario = usuario;
        this.saldo = saldo;
    }

    public String getUsuario() {
        return usuario;
    }

    public double getSaldo() {
        return saldo;
    }
}

