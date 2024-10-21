package distribuida.imd.domain;

import java.net.InetAddress;

public record ClientReplyAddress(InetAddress address, int port) {
}
