package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    int batchSize; // Number of tuples per out batch (i.e. no. tuples per page)

    int leftAttributeIndex;
    int rightAttributeIndex;

    String rightTableFileName;

    static int filenum = 0; // Get unique filenum for this operation

    Batch outputBuffer;
    ArrayList<Batch> leftBlockBuffers; // Block of buffers/batches
    Batch rightBuffer;
    ObjectInputStream rightTableFilePointer; // File pointer to right hand materialized file

    int leftBufferCursor;
    int leftCursor; // Cursor for left side buffer
    int rightCursor; // Cursor for right side buffer

    boolean endOfLeftTableStream;
    boolean endOfRightTableStream;

    public BlockNestedJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    public boolean open() {
        batchSize = Batch.getPageSize() / schema.getTupleSize(); // Set number of tuples per buffer/batch
        leftBlockBuffers = new ArrayList<Batch>(); // Initialize buffers/batches for left block


        // Set index of attributes used in join
        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();
        leftAttributeIndex = left.schema.indexOf(leftAttr);
        rightAttributeIndex = right.schema.indexOf(rightAttr);

        // Set cursors and boolean checkers
        leftBufferCursor = 0;
        leftCursor = 0;
        rightCursor = 0;
        endOfLeftTableStream = false;
        endOfRightTableStream = true;

        /** If the right operator is not a base table then
         ** Materialize the intermediate result from right
         ** into a file. From NestedJoin.java source
         **/
        Batch rightPage;

        if (!right.open()) {
            return false;
        } else {
            filenum++;
            rightTableFileName = "NJtemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightTableFileName));
                while((rightPage = right.next()) != null){
                    out.writeObject(rightPage);
                }
                out.close();
            } catch(IOException io){
                System.out.println("BlockNestedJoin:writing the temporary file error");
                return false;
            }
            if(!right.close()) {
                return false;
            }
        }

        if (left.open()) {
            return true;
        } else {
            return false;
        }
    }

    public Batch next() {
        if (endOfLeftTableStream) {
            close();
            return null;
        }

        outputBuffer = new Batch(batchSize);

        while (!outputBuffer.isFull()) {
            System.out.println("New iteration");
            // Load left buffers if right table stream has reached the end meaning we need new left block
            if (endOfRightTableStream && leftCursor == 0) {
                leftBlockBuffers.clear();
                while (leftBlockBuffers.size() <= numBuff - 2) {
                    Batch buffer = left.next();
                    if (buffer != null) {
                        System.out.println("Left Buffer is not null");
                        leftBlockBuffers.add(buffer);
                    } else {
                        System.out.println("Left Buffer is null");
                        break;
                    }
                }
                // Check if left block is empty. If it is then return output buffer
                if (leftBlockBuffers.isEmpty()) {
                    endOfLeftTableStream = true;
                    return outputBuffer;
                }
                endOfRightTableStream = false;
                try{
                    rightTableFilePointer = new ObjectInputStream(new FileInputStream(rightTableFileName));
                    endOfRightTableStream = false;
                }catch(IOException io){
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            // If left has reached the end, return output buffer anyways
            if (endOfLeftTableStream) {
                close();
                if (outputBuffer.isEmpty()) {
                    return null;
                } else {
                    return outputBuffer;
                }
            }
            System.out.println(leftBufferCursor);
            System.out.println("LeftBufferCursor: " + leftBufferCursor);
            while (!endOfRightTableStream) {
                System.out.println("Checkpoint 1 leftBufferCursor: "+ leftBufferCursor);
                try {
                    if (rightCursor == 0 && leftCursor == 0 && leftBufferCursor == 0) {
                        rightBuffer = (Batch) rightTableFilePointer.readObject();
                    }
                    for (int k = leftBufferCursor; k < leftBlockBuffers.size(); k++) {
                        Batch leftBuffer = leftBlockBuffers.get(k);
                        System.out.println("leftBufferCursor: " + leftBufferCursor + " leftCursor: " + leftCursor + " rightCursor: " + rightCursor);
                        for (int i = leftCursor; i < leftBuffer.size(); i++) {
                            System.out.println("Checkpoint 3");
                            Tuple lefttuple = leftBuffer.elementAt(i);
                            for (int j = rightCursor; j < rightBuffer.size(); j++) {
                                System.out.println("Checkpoint 4");
                                Tuple righttuple = rightBuffer.elementAt(j);
                                System.out.println("Comparing Left(" + leftBufferCursor + ", " + i + ", right: " + j);
                                if (lefttuple.checkJoin(righttuple, leftAttributeIndex, rightAttributeIndex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    System.out.println("Checkpoint 5");
                                    outputBuffer.add(outtuple);
                                    if (outputBuffer.isFull()) {
                                        System.out.println("Output buffer is full");
                                        if (k < leftBlockBuffers.size() - 1) { // left block still has buffer
                                            rightCursor = 0;
                                            leftCursor = 0;
                                            leftBufferCursor = k + 1;
                                        } else if (i < leftBuffer.size() - 1) { // current left buffer has not done
                                            rightCursor = 0;
                                            leftCursor = i + 1;
                                            leftBufferCursor = k;
                                        } else if (j < rightBuffer.size() - 1) { // Right table not done
                                            rightCursor += j + 1;
                                            leftCursor = i;
                                            leftBufferCursor = k;
                                        }
                                        return outputBuffer;
                                    }
                                }
                            }
                            rightCursor = 0;
                            System.out.println("Checkpoint 6");
                        }
                        leftCursor = 0;
                    }
                    System.out.println("Checkpoint 7");
                    leftBufferCursor = 0;
                } catch (EOFException e) {
                    try {
                        rightTableFilePointer.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin:Error in temporary file reading");
                    }
                    endOfRightTableStream = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin:temporary file reading error");
                    System.exit(1);
                }

            }

        }

        return outputBuffer;
    }

    public boolean close() {
        File file = new File(rightTableFileName);
        file.delete();
        left.close();
        right.close();
        return true;
    }
}
