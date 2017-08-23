import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
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

    private static final String VERTEX_CLASS = "TestVertexClass";

    private static final String VERTEX_ID = "vertexId";
    private static final String CREATOR_ID = "creatorId";
    private static final String BATCH_COUNT = "batchCount";
    private static final String ITERATION = "iteration";
    private static final String RND = "rnd";
    private static final String EDGE_LABEL = "connects";

    @Test
    public void mainTest() throws InterruptedException, ExecutionException {
        OClass clazz = createClass(factory.getNoTx());
        createProperties(clazz);
        createIndexes(clazz);

        addVertexesAndEdges(factory.getTx(), 1);

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
                    OrientGraph graph;
                    try {
                        while (!interrupt.get()) {
                            iterationNumber++;
                            graph = factory.getTx();
                            addVertexesAndEdges(graph, iterationNumber);
                            graph.shutdown();
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

    private OClass createClass(OrientGraphNoTx graphNoTx) {
        return graphNoTx.createVertexType(VERTEX_CLASS);
    }

    private void createProperties(OClass clazz) {
        clazz.createProperty(VERTEX_ID, OType.LONG);
        clazz.createProperty(CREATOR_ID, OType.INTEGER);
        clazz.createProperty(BATCH_COUNT, OType.INTEGER);
        clazz.createProperty(ITERATION, OType.LONG);
        clazz.createProperty(RND, OType.LONG);
    }

    private void createIndexes(OClass clazz) {
        clazz.createIndex(VERTEX_CLASS + "." + VERTEX_ID, OClass.INDEX_TYPE.NOTUNIQUE, VERTEX_ID);
        clazz.createIndex(VERTEX_CLASS + "." + RND, OClass.INDEX_TYPE.UNIQUE, RND);
        clazz.createIndex(VERTEX_CLASS + "." + ITERATION + "_" + CREATOR_ID,
                OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, ITERATION, CREATOR_ID);
    }

    private void addVertexesAndEdges(OrientGraph graph, long iterationNumber) {
        int batchSize = BasicUtils.generateBatchSize();
        List<Vertex> vertexes = new ArrayList<>(batchSize);
        List<Long> ids = new ArrayList<>();

        try {
            for (int i = 0; i < batchSize; i++) {
                Vertex vertex = graph.addVertex("class:" + VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_ID, Thread.currentThread().getId());
                vertex.setProperty(BATCH_COUNT, batchSize);
                vertex.setProperty(ITERATION, iterationNumber);
                vertex.setProperty(RND, BasicUtils.generateRnd());
                vertexes.add(vertex);
                int addedVertexes = vertexes.size();
                if (addedVertexes > 1) {
                    graph.addEdge(null, vertexes.get(i - 1), vertexes.get(i), EDGE_LABEL);
                }
                if (addedVertexes == batchSize) {
                    graph.addEdge(null, vertexes.get(i), vertexes.get(0), EDGE_LABEL);
                }
                if (addedVertexes == batchSize / 3 || addedVertexes == batchSize * 2 / 3 || addedVertexes == batchSize) {
                    performSelectOperations(graph, ids);
                }
            }
            graph.commit();
        } catch (Exception e) {
            graph.rollback();
        }
    }

    private void performSelectOperations(OrientGraph graph, List<Long> ids) {
        long firstId = ids.get(0);
        long lastId = ids.get(ids.size() - 1);
        long limit = lastId - (firstId + 1);

        graph.command(new OCommandSQL(
                "select * from V where vertexId <= " + lastId + " order by vertexId" + " limit " + limit)).execute();
    }

}