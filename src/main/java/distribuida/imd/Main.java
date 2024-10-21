package distribuida.imd;

import distribuida.imd.HTTP_TCP.HTTP_TCPGateway;
import distribuida.imd.UDP.UDPGateway;
import distribuida.imd.domain.Gateway;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error: Invalid Protocol Option");
            System.exit(0);
        }

        Gateway gateway = null;

        switch (args[0]) {
            case "UDP":
                gateway = new UDPGateway();
                break;
            case "TCP":
            case "HTML":
                gateway = new HTTP_TCPGateway("TCP");
                break;
            default:
                System.out.println("Error: Invalid Protocol Option");
                System.exit(0);
        }
        gateway.iniciar();
    }
}