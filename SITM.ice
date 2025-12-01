module SITM {

    struct ArcStat {
        int routeId;
        double sumSpeed;
        int count;
    };

    sequence<ArcStat> ArcStats;
    sequence<string> StringSeq;

    // ✅ Declaración del dictionary
    dictionary<int, double> IntDoubleMap;

    interface Master;

    interface Worker {
        void processStream(StringSeq lines, Master* cb);
    };

    // ---------- OBSERVER ----------
    interface TrafficSubscriber {
        void updateSpeeds(IntDoubleMap liveSpeeds);
    };

    interface Master {
        void registerWorker(Worker* w);
        void reportResult(int jobId, int chunkId, ArcStats stats);
        void jobFinished(int jobId);

        // Stream desde un cliente productor
        void ingestStream(StringSeq lines);

        // ---------- OBSERVER ----------
        void registerTrafficSubscriber(TrafficSubscriber* sub);
        void unregisterTrafficSubscriber(TrafficSubscriber* sub);
    };
};
