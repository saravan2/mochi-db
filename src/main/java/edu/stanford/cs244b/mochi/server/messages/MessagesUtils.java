package edu.stanford.cs244b.mochi.server.messages;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;

public class MessagesUtils {

    public static ProtocolMessage wrapIntoProtocolMessage(Object T) {
        return wrapIntoProtocolMessage(T, null);
    }

    public static ProtocolMessage wrapIntoProtocolMessage(Object T, String originalMessageUuid) {
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
        } else if (T instanceof HelloFromServer2.Builder) {
            pmBuilder.setHelloFromServer2((HelloFromServer2.Builder) T);
        } else if (T instanceof HelloFromServer2) {
            pmBuilder.setHelloFromServer2((HelloFromServer2) T);
        } else if (T instanceof HelloToServer2.Builder) {
            pmBuilder.setHelloToServer2((HelloToServer2.Builder) T);
        } else if (T instanceof HelloToServer2) {
            pmBuilder.setHelloToServer2((HelloToServer2) T);
        } else if (T instanceof ReadToServer.Builder) {
            pmBuilder.setReadToServer((ReadToServer.Builder) T);
        } else if (T instanceof ReadToServer) {
            pmBuilder.setReadToServer((ReadToServer) T);
        } else if (T instanceof Write1ToServer.Builder) {
            pmBuilder.setWrite1ToServer((Write1ToServer.Builder) T);
        } else if (T instanceof Write1ToServer) {
            pmBuilder.setWrite1ToServer((Write1ToServer) T);
        } else {
            throw new IllegalStateException(String.format("Invalit message of class %s: %s", T.getClass(), T));
        }
        pmBuilder.setMsgTimestamp(System.currentTimeMillis());
        pmBuilder.setMsgId(Utils.getUUID());
        if (originalMessageUuid != null) {
            pmBuilder.setReplyToMsgId(originalMessageUuid);
        }
        return pmBuilder.build();
    }
}
