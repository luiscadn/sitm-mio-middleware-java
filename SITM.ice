module SITM {
    
    struct ArcStat {
        int routeId; // ESTO DEBE SER routeId
        double sumSpeed;
        int count;
    };

    sequence<ArcStat> ArcStats;

    interface Master; 

    interface Worker {
        void processTask(int jobId, int chunkId, string fileName, long startOffset, long dataSize, Master* cb);
    };

    interface Master {
        void registerWorker(Worker* w);
        void reportResult(int jobId, int chunkId, ArcStats stats);
        void jobFinished(int jobId);
    };
};