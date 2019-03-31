package qp.utils;

import java.util.ArrayList;

public class Block {
    int maxCapacity;
    ArrayList<Batch> batchList = new ArrayList<Batch>();
    ArrayList<Tuple> tupleList = new ArrayList<Tuple>();


    public Block(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public void appendBatch(Batch batch) {
        batchList.add(batch);
    }

    public boolean blockIsFull() {
        return batchList.size() >= maxCapacity;
    }

    public ArrayList<Tuple> getTuples() {
        return tupleList;
    }
}
