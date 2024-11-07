package org.example;

import java.util.Set;
import org.antlr.v4.runtime.misc.Pair;

public class CorssNode {
    private int id;
    private Set<Pair<Integer, Integer>> outRoads; // Pair<网格号gridId, 路链号chainId>
    private Set<Pair<Integer, Integer>> inRoads;

    public CorssNode(int id, Set<Pair<Integer, Integer>> outRoads, Set<Pair<Integer, Integer>> inRoads) {
        this.id = id;
        this.outRoads = outRoads;
        this.inRoads = inRoads;
    }

    public int getId() {
        return id;
    }

    public Set<Pair<Integer, Integer>> getOutRoads() {
        return outRoads;
    }

    public Set<Pair<Integer, Integer>> getInRoads() {
        return inRoads;
    }


}
