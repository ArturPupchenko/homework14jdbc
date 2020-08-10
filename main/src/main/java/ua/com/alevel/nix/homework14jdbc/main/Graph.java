package ua.com.alevel.nix.homework14jdbc.main;

import java.util.HashMap;
import java.util.Map;

public class Graph {
    int numberOfVertices;
    Map<Integer, String> vertices;
    public int matrix[][];

    public Graph(Integer numberOfVertices) {
        this.numberOfVertices = numberOfVertices;
        vertices = new HashMap<>(numberOfVertices);
        matrix = new int[numberOfVertices][numberOfVertices];
    }

    public Graph(Integer numberOfVertices, Map<Integer, String> vertices) {
        this.numberOfVertices = numberOfVertices;
        this.vertices = vertices;
        matrix = new int[numberOfVertices][numberOfVertices];
    }

    public void setVertexName(Integer index, String name) {
        vertices.put(index, name);
    }

    public int getVertexIndex(String name) {
        int vertexIndex = 0;
        for (Map.Entry<Integer, String> entry : vertices.entrySet()) {
            if (entry.getValue().equals(name)) {
                vertexIndex = entry.getKey();
            }
        }
        return vertexIndex;
    }

    public void addEdge(int source, int destination, int weight) {
        matrix[source][destination] = weight;
        matrix[destination][source] = weight;
    }

    public int getMinimumVertex(boolean[] mst, int[] key) {
        int minKey = 20000;
        int vertex = -1;
        for (int i = 0; i < numberOfVertices; i++) {
            if (mst[i] == false && minKey > key[i]) {
                minKey = key[i];
                vertex = i;
            }
        }
        return vertex;
    }

    public int getShortestPathCost(int sourceVertexIndex, int targetVertexIndex) {
        int[] distance = dijkstra_GetMinDistances(sourceVertexIndex);
        int result = distance[targetVertexIndex];
        return result;
    }

    public int[] dijkstra_GetMinDistances(int sourceVertexIndex) {
        boolean[] spt = new boolean[numberOfVertices];
        int[] distance = new int[numberOfVertices];
        int INFINITY = 20000;


        for (int i = 0; i < numberOfVertices; i++) {
            distance[i] = 20000;
        }


        distance[sourceVertexIndex] = 0;


        for (int i = 0; i < numberOfVertices; i++) {


            int vertex_U = getMinimumVertex(spt, distance);


            spt[vertex_U] = true;


            for (int vertex_V = 0; vertex_V < numberOfVertices; vertex_V++) {

                if (matrix[vertex_U][vertex_V] > 0) {


                    if (spt[vertex_V] == false && matrix[vertex_U][vertex_V] != INFINITY) {

                        int newKey = matrix[vertex_U][vertex_V] + distance[vertex_U];
                        if (newKey < distance[vertex_V])
                            distance[vertex_V] = newKey;
                    }
                }
            }
        }
        return distance;
    }
}


