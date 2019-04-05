package qp.operators;

import java.util.Vector;

import qp.utils.Batch;

import qp.utils.*;


public class Distinct extends Operator{

    Operator base;
    int batchsize;  // number of tuples per outbatch
    int count = -1;

    public Distinct(Operator base, int type){
        super(type);
        this.base = base;

    }

    public void setBase(Operator base){
        this.base = base;
    }

    public Operator getBase(){
        return base;
    }

    /** Opens the connection to the base operator
     ** Then does the distinct operations
     **/

    public boolean open(){
        /** set number of tuples per page**/
        int tuplesize=schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;


        if(base.open())
                return true;
        else
                return false;
    }

    public Batch next() {

        Batch inbatch = base.next();
        count++;
        if(inbatch == null) {
            return null;
        }
        Batch outbatch = new Batch(batchsize);
        for (int a = 0; a < inbatch.size(); a++) {
            outbatch.add(inbatch.elementAt(a));
            for (int b = 0; b < outbatch.size() - 1; b++) {
                Tuple outBatchTup = outbatch.elementAt(b);
                System.out.println(b);
                if (outBatchTup.data().equals((inbatch.elementAt(a).data()))) {
                    outbatch.remove(outbatch.size() - 1);
                    System.out.println("addTup");
                    break;
                }
            }
        }
        base.open();
        Batch comparisonBatch = base.next();
        int comparisonCount = 0;
        while (comparisonBatch != null) {
            if (comparisonCount > count) {
                int k = 0;
                boolean found = false;
                while (k < outbatch.size()) {
                    found = false;
                    Tuple outBatchTup = outbatch.elementAt(k);
                    System.out.println("outBatchTup");
                    System.out.println(k);
                    System.out.println(outBatchTup.data());
                    for (int j = 0; j < comparisonBatch.size(); j++) {
                        Tuple tuptoCheck = comparisonBatch.elementAt(j);
                        System.out.println("tuptoCheck");
                        System.out.println(j);
                        System.out.println(tuptoCheck.data());
                        if (outBatchTup.data().equals(tuptoCheck.data())) {
                            System.out.println("removed outBatchTup");
                            outbatch.remove(outbatch.indexOf(outBatchTup));
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    } else {
                        k++;
                    }
                }
            }
            comparisonBatch = base.next();
            comparisonCount++;
        }
        base.open();
        System.out.println("open");
        comparisonBatch = base.next();
        comparisonCount = 0;
        while (comparisonCount < count) {
            comparisonBatch = base.next();
            comparisonCount++;
        }
        System.out.println("RETURN!!");
        return outbatch;
    }

    public Distinct clone(){
        Operator newbase = base.clone();
        Distinct newdist = new Distinct( base.clone(), optype);
        newdist.setSchema(newbase.getSchema());
        return newdist;
     }
}