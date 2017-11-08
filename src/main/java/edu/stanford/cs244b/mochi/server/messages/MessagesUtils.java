package edu.stanford.cs244b.mochi.server.messages;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;

public class MessagesUtils {

    public static ProtocolMessage wrapIntoProtocolMessage(Object T) {
        if (T == null) {
            throw new IllegalStateException("wrapIntoProtocolMessage does not support null");
        }
        ProtocolMessage.Builder pmBuilder = ProtocolMessage.newBuilder();
        if (T instanceof HelloFromServer.Builder) {
            pmBuilder.setHelloFromServer((HelloFromServer.Builder) T);
        } else if (T instanceof HelloFromServer) {
            pmBuilder.setHelloFromServer((HelloFromServer) T);
        } else if (T instanceof HelloToServer.Builder) {
            pmBuilder.setHelloToServer((HelloToServer.Builder) T);
        } else if (T instanceof HelloToServer) {
            pmBuilder.setHelloToServer((HelloToServer) T);
        } else {
            throw new IllegalStateException(String.format("Invalit message of class %s: %s", T.getClass(), T));
        }
        pmBuilder.setMsgTimestamp(System.currentTimeMillis());
        return pmBuilder.build();
    }
}
