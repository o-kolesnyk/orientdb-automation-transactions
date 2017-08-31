import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
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
        graph = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);

        OClass clazz = graph.createVertexClass(VERTEX_CLASS);
        graph.createEdgeClass(EDGE_LABEL);
        createProperties(clazz);
        createIndexes(clazz);
        graph.close();

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
                    try {
                        ODatabaseSession graph = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);
                        while (!interrupt.get()) {
                            iterationNumber++;
                            addVertexesAndEdges(graph, iterationNumber);
                        }
                        graph.close();

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

    private void addVertexesAndEdges(ODatabaseSession graph, long iterationNumber) {
        //TODO: uncomment this
        //int batchSize = BasicUtils.generateBatchSize();
        int batchSize = 6;
        List<OVertex> vertexes = new ArrayList<>(batchSize);
        List<Long> ids = new ArrayList<>();
        graph.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                OVertex vertex = graph.newVertex(VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_ID, Thread.currentThread().getId());
                vertex.setProperty(BATCH_COUNT, batchSize);
                vertex.setProperty(ITERATION, iterationNumber);
                vertex.setProperty(RND, BasicUtils.generateRnd());
                vertex.save();
                vertexes.add(vertex);
                int addedVertexes = vertexes.size();

                OEdge edge;
                if (addedVertexes > 1) {
                    edge = graph.newEdge(vertexes.get(i - 1), vertexes.get(i), EDGE_LABEL);
                    edge.save();
                }
                if (addedVertexes == batchSize) {
                    edge = graph.newEdge(vertexes.get(i), vertexes.get(0), EDGE_LABEL);
                    edge.save();
                }
                if (addedVertexes == batchSize / 3 || addedVertexes == batchSize * 2 / 3 || addedVertexes == batchSize) {
                    performSelectOperations(graph, ids, ids.size(), 1);
                }
                if (addedVertexes == batchSize) {
                    checkRingCreated(vertexes);
                }
            }
            graph.commit();

            //actions after commit
            performSelectOperations(graph, ids, ids.size(), 1);
            checkRingCreated(vertexes);
            checkClusterPositionsPositive(vertexes);
        } catch (Exception e) {
            graph.rollback();

            //actions after rollback
            performSelectOperations(graph, ids, 0, 0);
        }
    }

    private void performSelectOperations(ODatabaseSession graph, List<Long> ids, int expectedAll, int expectedUnique) {
        OResultSet allRecords = selectAllRecords(graph, ids);
        Assert.assertEquals(allRecords.stream().count(), expectedAll);

        List<OResultSet> byIds = selectById(graph, ids);
        for (OResultSet uniqueItem : byIds) {
            Assert.assertEquals(uniqueItem.stream().count(), expectedUnique);
        }
    }

    private OResultSet selectAllRecords(ODatabaseSession graph, List<Long> ids) {
        long firstId = ids.get(0);
        long lastId = ids.get(ids.size() - 1);
        long limit = lastId - firstId + 1;
        return graph.query(
                "select * from V where " + VERTEX_ID + " <= ? order by " + VERTEX_ID + " limit " + limit, lastId);
    }

    private List<OResultSet> selectById(ODatabaseSession graph, List<Long> ids) {
        List<OResultSet> results = new ArrayList<>();
        for (long id : ids) {
            results.add(graph.query("select from V where " + VERTEX_ID + " = ?", id));
        }
        return results;
    }

    private void checkRingCreated(List<OVertex> vertexes) {
        List<Integer> creatorIds = new ArrayList<>(vertexes.size());
        List<Long> iterationNumbers = new ArrayList<>(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            OVertex vertex = vertexes.get(i);
            creatorIds.add(vertex.getProperty(CREATOR_ID));
            iterationNumbers.add(vertex.getProperty(ITERATION));
            Iterable<OEdge> edges = vertex.getEdges(ODirection.OUT, EDGE_LABEL);
            OVertex connectedTo = edges.iterator().next().getTo();
            OVertex next;
            if (i == vertexes.size() - 1) {
                next = vertexes.get(0);
            } else {
                next = vertexes.get(i + 1);
            }
            boolean connected = connectedTo.getProperty(VERTEX_ID).equals(next.getProperty(VERTEX_ID));
            Assert.assertTrue("Vertexes are not correctly connected by edges", connected);
        }
        boolean isOneThread = creatorIds.stream().distinct().limit(2).count() <= 1;
        boolean isOneIteration = iterationNumbers.stream().distinct().limit(2).count() <= 1;
        Assert.assertTrue("Vertexes are not created by one thread", isOneThread);
        Assert.assertTrue("Vertexes are not created during one iteration", isOneIteration);
    }

    private void checkClusterPositionsPositive(List<OVertex> vertexes) {
        for (int i = 0; i < vertexes.size(); i++) {
            long clusterPosition = vertexes.get(i).getIdentity().getClusterPosition();
            Assert.assertTrue("Cluster position in a record is not positive",
                    clusterPosition >= 0);
        }
    }
}