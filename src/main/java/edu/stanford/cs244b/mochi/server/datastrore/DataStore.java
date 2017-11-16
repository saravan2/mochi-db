package edu.stanford.cs244b.mochi.server.datastrore;

import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.ReadToServer;
import edu.stanford.cs244b.mochi.server.messages.MochiProtocol.Write1ToServer;

public interface DataStore {
    public Object processReadRequest(ReadToServer readToServer);

    public Object processWrite1ToServer(Write1ToServer write1ToServer);
}
