package org.example;

public class RoadConnection {
    //网格号 路链号 索引号 路链长 路链方向
    private int gridId;
    private int chainId;
    private int index;
    private int length;
    private int direction;

    public RoadConnection(int gridId, int chainId, int index, int length, int direction) {
        this.gridId = gridId;
        this.chainId = chainId;
        this.index = index;
        this.length = length;
        this.direction = direction;
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
    public int getDirection() {
        return direction;
    }
}
