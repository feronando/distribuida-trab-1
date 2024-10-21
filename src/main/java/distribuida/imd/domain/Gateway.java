package distribuida.imd.domain;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Gateway {
    public Integer gatewayPort = 9000;
    public final Integer heartbeatPort = 9007;
    public final CopyOnWriteArrayList<Integer> liveServers;
    public AtomicInteger currServer;
    public ExecutorService executorService;

    public Gateway()  {
        this.liveServers = new CopyOnWriteArrayList<>();
        this.currServer = new AtomicInteger(-1);
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    public abstract void iniciar();

    public static boolean isClientRequest(String message) {
        String[] tokens = message.split(";");
        return tokens.length >= 3 && (tokens[0].equals("CRIAR") || tokens[0].equals("REMOVER") ||
                tokens[0].equals("SALDO") || tokens[0].equals("EXTRATO") ||
                (tokens[0].equals("DEPOSITAR") && tokens.length == 4) ||
                (tokens[0].equals("SACAR") && tokens.length == 4) ||
                (tokens[0].equals("TRANSFERIR") && tokens.length == 5));
    }
}
