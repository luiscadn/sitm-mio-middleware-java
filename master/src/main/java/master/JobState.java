package master;

import SITM.ArcStat;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

public class JobState {
    private final int jobId;
    private final int totalChunks;
    private final String outputPath;
    public long startTime;
    
    // Key: RouteID, Value: [SumSpeed, Count]
    private final ConcurrentHashMap<Integer, double[]> aggregated = new ConcurrentHashMap<>();
    private final AtomicInteger chunksReceived = new AtomicInteger(0);
    private boolean finished = false;

    public JobState(int jobId, int totalChunks, String outputPath) {
        this.jobId = jobId;
        this.totalChunks = totalChunks;
        this.outputPath = outputPath;
    }

    public void setStartTime(long t) { this.startTime = t; }

    public void addPartial(int chunkId, ArcStat[] stats) {
        for (ArcStat a : stats) {
            aggregated.compute(a.routeId, (k, v) -> {
                if (v == null) return new double[] { a.sumSpeed, (double) a.count };
                v[0] += a.sumSpeed;
                v[1] += a.count;
                return v;
            });
        }
        chunksReceived.incrementAndGet();
    }

    public boolean isComplete() {
        return chunksReceived.get() >= totalChunks;
    }

    public void aggregateAndPersist() {
        List<String> lines = new ArrayList<>();
        lines.add("routeId,avgSpeed,count");
        
        aggregated.forEach((routeId, val) -> {
            double avg = (val[1] > 0) ? (val[0] / val[1]) : 0.0;
            lines.add(routeId + "," + avg + "," + (int)val[1]);
        });

        try {
            Path outDir = Paths.get(outputPath);
            if (!Files.exists(outDir)) Files.createDirectories(outDir);
            Path file = outDir.resolve("job_" + jobId + "_results.csv");
            Files.write(file, lines);
            System.out.println("[JobState] Resultados guardados en: " + file.toAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void markFinished() { this.finished = true; }
    
    public boolean isFinished() {
            return finished;

        }
}