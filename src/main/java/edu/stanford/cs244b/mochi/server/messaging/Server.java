package edu.stanford.cs244b.mochi.server.messaging;

public class Server {
    private final String serverName;
    private final int port;

    public Server(String serverName, int port) {
        super();
        this.serverName = serverName;
        this.port = port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + port;
        result = prime * result + ((serverName == null) ? 0 : serverName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Server other = (Server) obj;
        if (port != other.port)
            return false;
        if (serverName == null) {
            if (other.serverName != null)
                return false;
        } else if (!serverName.equals(other.serverName))
            return false;
        return true;
    }

    public String getServerName() {
        return serverName;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Server [serverName=" + serverName + ", port=" + port + "]";
    }
}
