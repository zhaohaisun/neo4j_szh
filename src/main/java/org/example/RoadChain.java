package org.example;

import java.util.ArrayList;
import java.util.List;

public class RoadChain {
    //序号 网格号 路链号 索引号 路链长 等级 入链个数 出链个数 本链路方向
    private int id;
    private int gridId;
    private int chainId;
    private int index;
    private int length;
    private int level;
    private int inCount;
    private int outCount;
    private int direction;
    private List<RoadConnection> in_connections;
    private List<RoadConnection> out_connections;

    public RoadChain(int id, int gridId, int chainId, int index, int length, int level, int inCount, int outCount, int direction, List<RoadConnection> in_connections, List<RoadConnection> out_connections) {
        this.id = id;
        this.gridId = gridId;
        this.chainId = chainId;
        this.index = index;
        this.length = length;
        this.level = level;
        this.inCount = inCount;
        this.outCount = outCount;
        this.direction = direction;
        this.in_connections = new ArrayList<>();
        this.out_connections = new ArrayList<>();
    }

    public int getId() {
        return id;
    }
    public int getGridId() {
        return gridId;
    }
    public int getChainId() {
        return chainId;
    }
    public int getIndex() {
        return index;
    }
    public int getLength() {
        return length;
    }
    public int getLevel() {
        return level;
    }
    public int getInCount() {
        return inCount;
    }
    public int getOutCount() {
        return outCount;
    }
    public int getDirection() {
        return direction;
    }
    public List<RoadConnection> getInConnections() {
        return in_connections;
    }
    public List<RoadConnection> getOutConnections() {
        return out_connections;
    }

}
