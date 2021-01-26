package org.apache.pulsar.tests.integration.presto;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarSQLTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;

@Slf4j
public class TestPulsarSQLBase extends PulsarSQLTestSuite {

    protected void pulsarSQLBasicTest(TopicName topic, boolean isBatch, boolean useNsOffloadPolices) throws Exception {
        waitPulsarSQLReady();

        log.info("start prepare data for query.");
        int messageCnt = prepareData(topic, isBatch, useNsOffloadPolices);
        log.info("finish prepare data for query. messageCnt: {}", messageCnt);

        validateMetadata(topic);

        validateData(topic, messageCnt);
    }

    private void waitPulsarSQLReady() throws Exception {
        // wait until presto worker started
        ContainerExecResult result;
        do {
            try {
                result = execQuery("show catalogs;");
                assertThat(result.getExitCode()).isEqualTo(0);
                assertThat(result.getStdout()).contains("pulsar", "system");
                break;
            } catch (ContainerExecException cee) {
                if (cee.getResult().getStderr().contains("Presto server is still initializing")) {
                    Thread.sleep(10000);
                } else {
                    throw cee;
                }
            }
        } while (true);
    }

    protected int prepareData(TopicName topicName, boolean isBatch, boolean useNsOffloadPolices) throws Exception {
        throw new Exception("Unsupported operation prepareData.");
    }

    private void validateMetadata(TopicName topicName) throws Exception {
        ContainerExecResult result = execQuery("show schemas in pulsar;");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains(topicName.getNamespace());

        pulsarCluster.getBroker(0)
                .execCmd(
                        "/bin/bash",
                        "-c", "bin/pulsar-admin namespaces unload " + topicName.getNamespace());

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    ContainerExecResult r = execQuery(
                            String.format("show tables in pulsar.\"%s\";", topicName.getNamespace()));
                    assertThat(r.getExitCode()).isEqualTo(0);
                    assertThat(r.getStdout()).contains(topicName.getLocalName());
                }
        );
    }

    private void validateData(TopicName topicName, int messageNum) throws Exception {
        String namespace = topicName.getNamespace();
        String topic = topicName.getLocalName();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    ContainerExecResult containerExecResult = execQuery(
                            String.format("select * from pulsar.\"%s\".\"%s\" order by entryid;", namespace, topic));
                    assertThat(containerExecResult.getExitCode()).isEqualTo(0);
                    log.info("select sql query output \n{}", containerExecResult.getStdout());
                    String[] split = containerExecResult.getStdout().split("\n");
                    assertThat(split.length).isEqualTo(messageNum);
                    String[] split2 = containerExecResult.getStdout().split("\n|,");
                    for (int i = 0; i < messageNum; ++i) {
                        assertThat(split2).contains("\"" + i + "\"");
                        assertThat(split2).contains("\"" + "STOCK_" + i + "\"");
                        assertThat(split2).contains("\"" + (100.0 + i * 10) + "\"");
                    }
                }
        );

        // test predicate pushdown
        String url = String.format("jdbc:presto://%s",  pulsarCluster.getPrestoWorkerContainer().getUrl());
        Connection connection = DriverManager.getConnection(url, "test", null);

        String query = String.format("select * from pulsar" +
                ".\"%s\".\"%s\" order by __publish_time__", namespace, topic);
        log.info("Executing query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);

        List<Timestamp> timestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            timestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(timestamps.size()).isGreaterThan(messageNum - 2);

        query = String.format("select * from pulsar" +
                ".\"%s\".\"%s\" where __publish_time__ > timestamp '%s' order by __publish_time__", namespace, topic, timestamps.get(timestamps.size() / 2));
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        List<Timestamp> returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size() + 1).isEqualTo(timestamps.size() / 2);

        // Try with a predicate that has a earlier time than any entry
        // Should return all rows
        query = String.format("select * from pulsar.\"%s\".\"%s\" where "
                + "__publish_time__ > from_unixtime(%s) order by __publish_time__", namespace, topic, 0);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size());

        // Try with a predicate that has a latter time than any entry
        // Should return no rows

        query = String.format("select * from pulsar.\"%s\".\"%s\" where "
                + "__publish_time__ > from_unixtime(%s) order by __publish_time__", namespace, topic, 99999999999L);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(0);
    }

    public static ContainerExecResult execQuery(final String query) throws Exception {
        ContainerExecResult containerExecResult;

        containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");

        Stopwatch sw = Stopwatch.createStarted();
        while (containerExecResult.getExitCode() != 0 && sw.elapsed(TimeUnit.SECONDS) < 120) {
            TimeUnit.MILLISECONDS.sleep(500);
            containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                    .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");
        }

        return containerExecResult;

    }

    private static void printCurrent(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = rs.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
        }
        System.out.println("");

    }


}
