package edu.stanford.cs244b.mochi.server.messages;

import edu.stanford.cs244b.mochi.server.Utils;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloFromServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.HelloToServer2;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ProtocolMessage;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.RequestFailedFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1OkFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1RefusedFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2AnsFromServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write2ToServer;

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
        } else if (T instanceof ReadFromServer.Builder) {
            pmBuilder.setReadFromServer((ReadFromServer.Builder) T);
        } else if (T instanceof ReadFromServer) {
            pmBuilder.setReadFromServer((ReadFromServer) T);
        } else if (T instanceof Write1ToServer.Builder) {
            pmBuilder.setWrite1ToServer((Write1ToServer.Builder) T);
        } else if (T instanceof Write1ToServer) {
            pmBuilder.setWrite1ToServer((Write1ToServer) T);
        } else if (T instanceof Write1OkFromServer.Builder) {
            pmBuilder.setWrite1OkFromServer((Write1OkFromServer.Builder) T);
        } else if (T instanceof Write1OkFromServer) {
            pmBuilder.setWrite1OkFromServer((Write1OkFromServer) T);
        } // ---- Write1RefusedFromServer
        else if (T instanceof Write1RefusedFromServer.Builder) {
            pmBuilder.setWrite1RefusedFromServer((Write1RefusedFromServer.Builder) T);
        } else if (T instanceof Write1RefusedFromServer) {
            pmBuilder.setWrite1RefusedFromServer((Write1RefusedFromServer) T);
        }
        // ---- Write2ToServer
        else if (T instanceof Write2ToServer.Builder) {
            pmBuilder.setWrite2ToServer((Write2ToServer.Builder) T);
        } else if (T instanceof Write2ToServer) {
            pmBuilder.setWrite2ToServer((Write2ToServer) T);
        }
        // ---- Write2AnsFromServer
        else if (T instanceof Write2AnsFromServer.Builder) {
            pmBuilder.setWrite2AnsFromServer((Write2AnsFromServer.Builder) T);
        } else if (T instanceof Write2AnsFromServer) {
            pmBuilder.setWrite2AnsFromServer((Write2AnsFromServer) T);
        }
        // ---- RequestFailedFromServer
        else if (T instanceof RequestFailedFromServer.Builder) {
            pmBuilder.setRequestFailedFromServer((RequestFailedFromServer.Builder) T);
        } else if (T instanceof RequestFailedFromServer) {
            pmBuilder.setRequestFailedFromServer((RequestFailedFromServer) T);
        }// ---
        else {
            throw new IllegalStateException(String.format("Invalid message of class %s: %s", T.getClass(), T));
        }
        pmBuilder.setMsgTimestamp(System.currentTimeMillis());
        pmBuilder.setMsgId(Utils.getUUID());
        if (originalMessageUuid != null) {
            pmBuilder.setReplyToMsgId(originalMessageUuid);
        }
        return pmBuilder.build();
    }
}
