module SITM {
    
    struct ArcStat {
        int routeId;
        double sumSpeed;
        int count;
    };

    sequence<ArcStat> ArcStats;
    sequence<string> StringSeq; // Nuevo: Para enviar lotes de líneas CSV

    interface Master; 

    interface Worker {
        void processTask(int jobId, int chunkId, string fileName, long startOffset, long dataSize, Master* cb);
        
        // NUEVO: Procesa un lote de líneas en memoria
        void processStream(StringSeq lines, Master* cb);
    };

    interface Master {
        void registerWorker(Worker* w);
        void reportResult(int jobId, int chunkId, ArcStats stats);
        void jobFinished(int jobId);

        // NUEVO: Recibe datos en vivo desde el cliente
        void ingestStream(StringSeq lines);
    };
};