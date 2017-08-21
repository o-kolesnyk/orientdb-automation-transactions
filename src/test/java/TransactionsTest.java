import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.testng.annotations.Test;
import utils.BasicUtils;
import utils.Counter;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static utils.BasicUtils.getTimeToInterrupt;

public class TransactionsTest extends CreateGraphDatabaseFixture {

    @Test
    public void mainTest() throws InterruptedException, ExecutionException {

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Callable<Object>> tasks = new ArrayList<>();
        AtomicBoolean interrupt = new AtomicBoolean(false);

        new Timer().schedule(
                new TimerTask() {
                    public void run() {
                        interrupt.set(true);
                    }
                },
                getTimeToInterrupt());

        try {
            for (int i = 0; i < 8; i++) {
                tasks.add(() -> {
                    long iterationNumber = 0;
                    OrientGraph database;
                    try {
                        while (!interrupt.get()) {
                            iterationNumber++;
                            database = factory.getTx();
                            addVertexesAndEdges(database, iterationNumber);
                            database.shutdown();
                        }
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }
            List<Future<Object>> futures = executor.invokeAll(tasks);
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }

    private void addVertexesAndEdges(OrientGraph database, long iterationNumber) {
        int size = BasicUtils.generateBatchSize();
        long id = Counter.getNextVertexId();
        List<Vertex> vertexes = new ArrayList<>(size);

        try {
            for (int i = 0; i < size; i++) {
                Vertex vertex = database.addVertex(null);
                vertex.setProperty("vertexId", id);
                vertex.setProperty("creatorId", Thread.currentThread().getId());
                vertex.setProperty("batchCount", size);
                vertex.setProperty("iteration", iterationNumber);
                vertexes.add(vertex);
            }

            for (int i = 0; i < vertexes.size(); i++) {
                int next = i + 1;
                if (next >= size) {
                    next = 0;
                }
                database.addEdge(null, vertexes.get(i), vertexes.get(next), "connects");
            }

            database.commit();
        } catch (Exception e) {
            database.rollback();

        }
    }

}