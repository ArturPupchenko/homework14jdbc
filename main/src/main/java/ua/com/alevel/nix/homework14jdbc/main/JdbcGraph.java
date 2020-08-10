package ua.com.alevel.nix.homework14jdbc.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.*;

public class JdbcGraph {

    private static final Logger log = LoggerFactory.getLogger(JdbcGraph.class);

    public static void main(String[] args) {

        Properties props = loadProperties();

        String url = props.getProperty("url");

        log.info("Connecting to {}", url);

        insertLocationsToDatabase(props, url);
        insertRoutesToDatabase(props, url);
        insertProblemsToDatabase(props, url);

        Graph graph = buildGraphFromDatabase(props, url);
        Map<Integer, Integer> problems = getProblemsFromDatabase(props, url);
        List<Integer> solutions = resolveProblems(props, url, graph, problems);
        insertSolutionsToDatabase(props, url, graph, solutions);
//        resetRoutesAndSolutionsTable(props, url);
    }

    private static Properties loadProperties() {

        Properties props = new Properties();

        try (InputStream input = JdbcGraph.class.getResourceAsStream("/JdbcGraph.properties")) {
            props.load(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return props;
    }

    public static void insertLocationsToDatabase(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (PreparedStatement insertLocations = connection.prepareStatement(
                    "INSERT INTO locations (name) VALUES (?) ON CONFLICT DO NOTHING;",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {

                insertLocations.setString(1, "gdansk");
                insertLocations.addBatch();

                insertLocations.setString(1, "bydgoszcz");
                insertLocations.addBatch();

                insertLocations.setString(1, "torun");
                insertLocations.addBatch();

                insertLocations.setString(1, "warszawa");
                insertLocations.addBatch();

                insertLocations.executeBatch();

                ResultSet generatedKeys = insertLocations.getGeneratedKeys();

                while (generatedKeys.next()) {
                    log.info("inserted new locations. ID : {}", generatedKeys.getLong("id"));
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void insertRoutesToDatabase(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (PreparedStatement insertRoutes = connection.prepareStatement(
                    "INSERT INTO routes (from_id,to_id,cost) VALUES (?,?,?) ON CONFLICT DO NOTHING;",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {

                insertRoutes.setInt(1, 1);
                insertRoutes.setInt(2, 2);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 1);
                insertRoutes.setInt(2, 3);
                insertRoutes.setInt(3, 3);
                insertRoutes.addBatch();


                insertRoutes.setInt(1, 2);
                insertRoutes.setInt(2, 1);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 2);
                insertRoutes.setInt(2, 3);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 2);
                insertRoutes.setInt(2, 4);
                insertRoutes.setInt(3, 4);
                insertRoutes.addBatch();


                insertRoutes.setInt(1, 3);
                insertRoutes.setInt(2, 1);
                insertRoutes.setInt(3, 3);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 3);
                insertRoutes.setInt(2, 2);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 3);
                insertRoutes.setInt(2, 4);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();


                insertRoutes.setInt(1, 4);
                insertRoutes.setInt(2, 2);
                insertRoutes.setInt(3, 4);
                insertRoutes.addBatch();

                insertRoutes.setInt(1, 4);
                insertRoutes.setInt(2, 3);
                insertRoutes.setInt(3, 1);
                insertRoutes.addBatch();


                insertRoutes.executeBatch();

                ResultSet generatedKeys = insertRoutes.getGeneratedKeys();

                while (generatedKeys.next()) {
                    log.info("inserted new routes. ID : {}", generatedKeys.getLong("id"));
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void insertProblemsToDatabase(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (PreparedStatement insertProblems = connection.prepareStatement(
                    "INSERT INTO problems (id,from_id,to_id) VALUES (?,?,?) ON CONFLICT DO NOTHING;",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {


                insertProblems.setInt(1, 1);
                insertProblems.setInt(2, 1);
                insertProblems.setInt(3, 4);
                insertProblems.addBatch();

                insertProblems.setInt(1, 2);
                insertProblems.setInt(2, 2);
                insertProblems.setInt(3, 4);
                insertProblems.addBatch();


                insertProblems.executeBatch();
                ResultSet generatedKeys = insertProblems.getGeneratedKeys();

                while (generatedKeys.next()) {
                    log.info("inserted new problems. ID : {}", generatedKeys.getLong("id"));
                }
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }


    }

    public static Graph buildGraphFromDatabase(Properties props, String url) {

        Map<Integer, String> locations = getLocationsFromDatabase(props, url);
        int numberOfLocations = locations.size();
        Graph graph = new Graph(numberOfLocations, locations);
        getRoutesFromDatabase(props, url, graph);
        return graph;
    }

    private static Map<Integer, String> getLocationsFromDatabase(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (Statement getLocations = connection.createStatement()) {
                ResultSet resultSet = getLocations.executeQuery("Select name FROM locations;");
                Map<Integer, String> locations = new LinkedHashMap<>();
                int i = 0;
                while (resultSet.next()) {
                    locations.put(i, resultSet.getString("name"));
                    i++;
                }
                return locations;
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void getRoutesFromDatabase(Properties props, String url, Graph graph) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (Statement getRoutes = connection.createStatement()) {
                ResultSet resultSet = getRoutes.executeQuery("Select from_id,to_id,cost FROM routes;");
                while (resultSet.next()) {
                    int from = resultSet.getInt("from_id") - 1;
                    int to = resultSet.getInt("to_id") - 1;
                    int cost = resultSet.getInt("cost");
                    graph.matrix[from][to] = cost;
                }
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Integer, Integer> getProblemsFromDatabase(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (Statement getProblems = connection.createStatement()) {
                ResultSet resultSet = getProblems.executeQuery("Select from_id,to_id FROM problems;");
                Map<Integer, Integer> problems = new HashMap<>();
                while (resultSet.next()) {
                    int from = resultSet.getInt("from_id") - 1;
                    int to = resultSet.getInt("to_id") - 1;
                    problems.put(from, to);
                }
                return problems;
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Integer> resolveProblems(Properties props, String url, Graph graph, Map<Integer, Integer> problems) {
        List<Integer> solutions = new ArrayList<>();
        for (Map.Entry<Integer, Integer> problem : problems.entrySet()) {
            int from = problem.getKey();
            int to = problem.getValue();
            int result = graph.getShortestPathCost(from, to);
            solutions.add(result);
        }
        return solutions;
    }

    private static void insertSolutionsToDatabase(Properties props, String url, Graph graph, List<Integer> solutions) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (PreparedStatement insertSolutions = connection.prepareStatement(
                    "INSERT INTO solutions (cost) VALUES (?) ON CONFLICT DO NOTHING;")) {
                for (Integer solution : solutions) {
                    insertSolutions.setInt(1, solution);
                    insertSolutions.addBatch();
                }
                insertSolutions.executeBatch();
            }
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void resetRoutesAndSolutionsTable(Properties props, String url) {
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (Statement resetTable = connection.createStatement()) {
                resetTable.executeUpdate("TRUNCATE TABLE routes RESTART IDENTITY;");
                log.info("Table routes was reset!");
            }
            try (Statement resetTable = connection.createStatement()) {
                resetTable.executeUpdate("TRUNCATE TABLE solutions RESTART IDENTITY;");
                log.info("Table solutions was reset!");
            }

        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
