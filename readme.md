# CS3223 Project
## Co-authors
GOH CHANG KANG, CHARLES <br>
JEFFREY GOH <br>
ZHU YILUN <br>

# Implementation
## Block Nested Join
### BlockNestedJoin.java
```java
public class BlockNestedJoin extends Join {
	// ...Implementation
}
```
The above code was implemented under the BlockNestedJoin.java file, located in the operators folder where all the join operators are located.
```java
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
```
First, we declare the required variables to be used such as the memory statistics variables, the block to be used (as an ArrayList of buffer Batches), the right relation buffer, the object input stream for the right relation and the required state variables like the block buffer pointer and the tuple pointers in the currently accessed buffers.
```java
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
		rightTableFileName = "BNJtemp-" + String.valueOf(filenum);
		try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightTableFileName));
			while ((rightPage = right.next()) != null) {
				out.writeObject(rightPage);
			}
			out.close();
		} catch (IOException io) {
			System.out.println("BlockNestedJoin:writing the temporary file error");
			return false;
		}
		if (!right.close()) {
			return false;
		}
	}

	if (left.open()) {
		return true;
	} else {
		return false;
	}
}
```
We then implemented the open() method. Using a similar method as the open() method in the NestedJoin.java operator, we set the attributes, reset the cursor/state pointers, and prepare a new page for the right relation's buffer.
```java
public Batch next() {
	if (endOfLeftTableStream) {
		close();
		return null;
	}

	outputBuffer = new Batch(batchSize);

	while (!outputBuffer.isFull()) {
		//System.out.println("New iteration");
		// Load left buffers if right table stream has reached the end meaning we need new left block
		if (endOfRightTableStream && leftCursor == 0) {
			leftBlockBuffers.clear();
			while (leftBlockBuffers.size() <= numBuff - 2) {
				Batch buffer = left.next();
				if (buffer != null) {
					//System.out.println("Left Buffer is not null");
					leftBlockBuffers.add(buffer);
				} else {
					//System.out.println("Left Buffer is null");
					break;
				}
			}
			// Check if left block is empty. If it is then return output buffer
			if (leftBlockBuffers.isEmpty()) {
				endOfLeftTableStream = true;
				return outputBuffer;
			}
			endOfRightTableStream = false;
			try {
				rightTableFilePointer = new ObjectInputStream(new FileInputStream(rightTableFileName));
				endOfRightTableStream = false;
			} catch (IOException io) {
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
		//System.out.println(leftBufferCursor);
		//System.out.println("LeftBufferCursor: " + leftBufferCursor);
		while (!endOfRightTableStream) {
			//System.out.println("Checkpoint 1 leftBufferCursor: " + leftBufferCursor);
			try {
				if (rightCursor == 0 && leftCursor == 0 && leftBufferCursor == 0) {
					rightBuffer = (Batch) rightTableFilePointer.readObject();
				}
				for (int k = leftBufferCursor; k < leftBlockBuffers.size(); k++) {
					Batch leftBuffer = leftBlockBuffers.get(k);
					//System.out.println("leftBufferCursor: " + leftBufferCursor + " leftCursor: " + leftCursor + " rightCursor: " + rightCursor);
					for (int i = leftCursor; i < leftBuffer.size(); i++) {
						//System.out.println("Checkpoint 3");
						Tuple lefttuple = leftBuffer.elementAt(i);
						for (int j = rightCursor; j < rightBuffer.size(); j++) {
							//System.out.println("Checkpoint 4");
							Tuple righttuple = rightBuffer.elementAt(j);
							//System.out.println("Comparing Left(" + leftBufferCursor + ", " + i + ", right: " + j);
							if (lefttuple.checkJoin(righttuple, leftAttributeIndex, rightAttributeIndex)) {
								Tuple outtuple = lefttuple.joinWith(righttuple);
								//System.out.println("Checkpoint 5");
								outputBuffer.add(outtuple);
								if (outputBuffer.isFull()) {
									//System.out.println("Output buffer is full");
									if (j < rightBuffer.size() - 1) { // Right table not done
										rightCursor += j + 1;
										leftCursor = i;
										leftBufferCursor = k;
									} else if (i < leftBuffer.size() - 1) { // current left buffer has not done
										rightCursor = 0;
										leftCursor = i + 1;
										leftBufferCursor = k;
									} else if (k < leftBlockBuffers.size() - 1) { // left block still has buffer
										rightCursor = 0;
										leftCursor = 0;
										leftBufferCursor = k + 1;
									}
									return outputBuffer;
								}
							}
						}
						rightCursor = 0;
						//System.out.println("Checkpoint 6");
					}
					leftCursor = 0;
				}
				//System.out.println("Checkpoint 7");
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
```
In the next() function, while the block for the left relation is not full we keep adding pages to it. Then, we iterate through the pages and tuples of the block, each time reading in one page at a time from the right relation and joining its tuples with the former’s, all the while keeping track of the “address” of both relations. I.e. for the current block, which page it is on, which tuple it is on and for the right relation’s current buffer, which tuple it is on. When the output buffer/page is full, we keep track of the current state address of the left and right side before allowing the object to call next() for the next output page.
```java
public boolean close() {
	File file = new File(rightTableFileName);
	file.delete();
	left.close();
	right.close();
	return true;
}
```
Here we deallocate the space by closing both relations.
## Hash Join
### HashJoin.java

Hash Join is implemented in HashJoin.java, located in the same folder as the rest of the joins.
```java
package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HashJoin extends Join{

	int batchsize;  //Number of tuples per out batch
	int rightbatchsize; //number of tuples per batch right
	int leftbatchsize; //number of tuples per batch left

	int leftindex;     // Index of the join attribute in left table
	int rightindex;    // Index of the join attribute in right table

	String rfname;    // right partition file name
	String lfname;    // left partition file name

	Batch[] join_hash; //in memory hash table

	Batch outbatch;   // Output buffer
	Batch leftpart;  // Buffer for left input stream
	Batch rightpart;  // Buffer for right input stream
	ObjectInputStream in_left; // File pointer to the left partition
	ObjectInputStream in_right; // File pointer to the right partition


	int lcurs;    // Cursor for left side buffer
	int rcurs;    // Cursor for right side buffer
	int partition_no; // to keep track of hj partitions
	boolean eosl;  // Whether end of stream (left table) is reached
	boolean eosr;  // End of stream (right table)
	boolean hjfin; //End of hashjoin

	public HashJoin(Join jn){
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();

	}

```
HashJoin is split into two phases, partition and probe. In Open(), we are partitioning both the left and right table into X buckets. Here, the hash function is Attribute.hashcode() % numBuckets, where numBuckets is the number of buffers – 1. Attributes will be assigned to buckets depending on their hashcode. This partitioning is done separately for table A and table B, with the same hash method. Whenever the partition bucket is full, we will write it out to a file tagged with the hashcode of the table and the bucket number, and initialise a new bucket to continue the partition process.

```java
public boolean open(){


	int tuplesize=schema.getTupleSize();
	batchsize=Batch.getPageSize()/tuplesize;
	int righttuplesize = right.schema.getTupleSize();
	rightbatchsize = Batch.getPageSize()/righttuplesize;
	int lefttuplesize = left.schema.getTupleSize();
	leftbatchsize = Batch.getPageSize()/lefttuplesize;

	Batch[] partbuckets = new Batch[numBuff - 1]; //num of partitions = B-1

	Attribute leftattr = con.getLhs();
	Attribute rightattr =(Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);
	lcurs = 0; rcurs =0;
	partition_no = -1;
	eosl=true;
	eosr=true;
	hjfin = false;

	System.out.println("Initiating hash join: Partition phase");

	//left partition
	if (!left.open()) {
		return false;
	}
	else {
		//initialise buckets for partitioning
		for (int i = 0; i < partbuckets.length; i++) {
			partbuckets[i] = new Batch(leftbatchsize);
		}
		try{
			ObjectOutputStream[] leftoutput = new ObjectOutputStream[numBuff - 1];
			for(int i = 0; i<numBuff-1;i++){
				//identify partitions by the bucket number i
				//hashcode is taken to differentiate files when there are multiple joins
				String fname =  "HJLeft" + i + this.hashCode();
				leftoutput[i] = new ObjectOutputStream(new FileOutputStream(fname));
			}
			while ((leftpart = left.next()) != null) {
				for (int i = 0; i < leftpart.size(); i++) {
					Tuple temp = leftpart.elementAt(i);
					int key = temp.dataAt(leftindex).hashCode();
					//hash function key % numbuckets
					int bucketnum = key % (partbuckets.length);

					//if bucket is full, write out to disk, initialise new bucket to continue
					if (partbuckets[bucketnum].isFull()) {
						leftoutput[bucketnum].writeObject(partbuckets[bucketnum]);
						partbuckets[bucketnum] = new Batch(leftbatchsize);
					}
					partbuckets[bucketnum].add(temp);
				}
			}

			for(int i = 0; i < numBuff - 1;i++){
				if(!partbuckets[i].isEmpty()){
					leftoutput[i].writeObject(partbuckets[i]); //at the end, write the rest into file
				}
			}
			//close output stream
			for(int i = 0; i < numBuff -1 ;i++){
				leftoutput[i].close();
			}

		}catch(IOException io){
			System.out.println("HashJoin: Partition left error");
			return false;
		}
		if(!left.close()) {
			return false;
		}
	}
	//right partition, same as left
	if (!right.open()) {
		return false;
	}
	else {
		//initialise buckets for partition
		for (int i = 0; i < partbuckets.length; i++) {
			partbuckets[i] = new Batch(rightbatchsize);
		}
		try{
			ObjectOutputStream[] rightoutput = new ObjectOutputStream[numBuff - 1];
			for(int i = 0; i < numBuff - 1; i++){
				String fname =  "HJRight" + i + this.hashCode();
				rightoutput[i] = new ObjectOutputStream(new FileOutputStream(fname));
			}
			while ((rightpart = right.next()) != null) {
				for (int i = 0; i < rightpart.size(); i++) {
					Tuple temp = rightpart.elementAt(i);
					int key = temp.dataAt(rightindex).hashCode();
					int bucketnum = key % (partbuckets.length);

					//if bucket is full, write out to disk
					if (partbuckets[bucketnum].isFull()) {
						rightoutput[bucketnum].writeObject(partbuckets[bucketnum]);
						partbuckets[bucketnum] = new Batch(rightbatchsize);
					}
					partbuckets[bucketnum].add(temp);
				}
			}

			for(int i = 0; i < numBuff - 1;i++){
				if(!partbuckets[i].isEmpty()){
					rightoutput[i].writeObject(partbuckets[i]); //write the rest into file
				}
			}

			//close output stream
			for(int i = 0; i < numBuff -1 ;i++){
				rightoutput[i].close();
			}

		}catch(IOException io){
			System.out.println("HashJoin: Partition right error" );
			io.printStackTrace();
			return false;
		}
		return right.close();
	}


}
```
The probing phase occurs during next(). When next() is called, it will carry out the probing until the current outbatch is full, or when the probing phase ends. During the probing phase, we will load the left partition into a Batch file joinHash that represents the in-memory hash table with number of buffers – 2 buckets using the hash function %(numBuffers-2).
```java

	//carry out probing phase
	public Batch next() {
		if (hjfin){
			System.out.println("HashJoin:-----------------FINISHED----------------");
			close();
			return null;
		}
		outbatch = new Batch(batchsize);
		//carry out until out buffer is full
		while (!outbatch.isFull()) {
			if (lcurs == 0 && rcurs == 0 && eosr) { //start or end of last batch

				if (partition_no == numBuff - 2 && eosl) {
					//all done
					hjfin = true;
					close();
					if (!outbatch.isEmpty()) {
						return outbatch;
					} else {
						return null;
					}

				} else {
					//if it is the end of last batch, continue with next partition
					//else, continue reading current partition
					if (eosl) {
						partition_no++;
						lfname = "HJLeft" + partition_no + this.hashCode();
						rfname = "HJRight" + partition_no + this.hashCode();
					}

					try {
						in_left = new ObjectInputStream(new FileInputStream(lfname));
						in_right = new ObjectInputStream(new FileInputStream(rfname));
					} catch (IOException e) {
						System.out.println("Join file input failed");
						e.printStackTrace();
						continue;
					}
					join_hash = new Batch[numBuff - 2]; //initiate new hashtable for probing

					//initialise new buffer pages for hash table
					for (int i = 0; i < numBuff - 2; i++) {
						join_hash[i] = new Batch(leftbatchsize);
					}
					eosl = false;
					eosr = false;
					rightpart = null;
					boolean full = false;
					//read until left partition reaches the end
					while (!eosl && !full) {
						try {
							leftpart = (Batch) in_left.readObject();
							while (leftpart.isEmpty() || leftpart == null) {
								leftpart = (Batch) in_left.readObject();
							}
						} catch (EOFException e) {
							eosl = true;
							try {
								in_left.close();
							} catch (Exception in) {
								in.printStackTrace();
							}
							break;

						} catch (Exception e) {
							e.printStackTrace();
							System.exit(1);
						}
						for (int left_pt = 0; left_pt < leftpart.size(); left_pt++) { //build hash table with left partition
							Tuple temp = leftpart.elementAt(left_pt);
							int key = temp.dataAt(leftindex).hashCode();
							//hash function, different from partition phase
							int bucketnum = key % (numBuff - 2);
							join_hash[bucketnum].add(temp);
```
If the hashtable bucket is not big enough to fit the entire partition of the left table, we will write out whatever that is remaining in the partition to a file, and continue the probing phase. In the next round, we will load and finish the remaining tuples of the left table that could not fit into the hash table.
```java
				if (join_hash[bucketnum].size() >= leftbatchsize) {
					//if buffer is not big enough, write the rest into file and continue reading next round.
					eosl = false;
					String tempFile = "tempFile" + this.hashCode();
					full = true;
					try {
						ObjectOutputStream tempOut = new ObjectOutputStream(new FileOutputStream(tempFile));
						if (left_pt <= leftpart.size() - 1) {
							for (int i = 0; i <= left_pt; i++) {
								leftpart.remove(0);
							}
							//store the remaining data into temp file
							if (!leftpart.isEmpty() || leftpart == null) {
								tempOut.writeObject(leftpart);
							}
						}
						boolean done = false;

						//store the remaining data in stream to tempfile
						while (!done) {
							try {
								tempOut.writeObject(in_left.readObject());
							} catch (EOFException e) {
								in_left.close();
								tempOut.close();
								File f = new File(lfname); //remove current file
								done = true;
								f.delete();
							}
						}
						//create a new file with the same file name, and write in the remaining data
						ObjectOutputStream out_left = new ObjectOutputStream(new FileOutputStream(lfname));
						ObjectInputStream temp_in = new ObjectInputStream(new FileInputStream(tempFile));
						done = false;

						while (!done) {
							try {
								//write data from temp file to new file
								out_left.writeObject(temp_in.readObject());
							} catch (EOFException io) {
								done = true;
								temp_in.close();
								out_left.close();
								File f = new File(tempFile);
								f.delete();
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		//prepare one right partition for probing
		try {
			rightpart = (Batch) in_right.readObject();
		} catch (EOFException e) {
			//if there is no right partition, there is nothing to be matched with. done.
			eosr = true;
			lcurs = 0;
			rcurs = 0;
			try {
				in_right.close();
			} catch (Exception in) {
				in.printStackTrace();
			}
			continue;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
```
During probing, tuples from the right table of the same partition number is read, and hashed by %(numBuffers-2), to check against the left table tuples in the same joinHash bucket. If the attribute matches, it is written into the outbatch. After both left and right partitions are finished, we will move on to the next partition.

```java
//reading right partition until it finishes
while (!eosr) {
	//load hashtable for probing
	for (int right_pt = rcurs; right_pt < rightpart.size(); right_pt++) {
		Tuple rightTemp = rightpart.elementAt(right_pt);
		int key = rightTemp.dataAt(rightindex).hashCode();
		int bucketnum = key % (numBuff - 2);
		//probe against left partition in hash table
		for (int tbl_pt = lcurs; tbl_pt < join_hash[bucketnum].size(); tbl_pt++) {
			Tuple leftTemp = join_hash[bucketnum].elementAt(tbl_pt);
			if (leftTemp.checkJoin(rightTemp, leftindex, rightindex)) {
				Tuple outtuple = leftTemp.joinWith(rightTemp);
				//if it matches, write to outbatch
				outbatch.add(outtuple);
				if (outbatch.isFull()) {
					//if outbatch is full, write out and save pointers for next round
					if (tbl_pt == join_hash[bucketnum].size() - 1 && right_pt != rightpart.size() - 1) {
						lcurs = 0;
						rcurs = right_pt + 1;
					}
					else {
						lcurs = tbl_pt + 1;
						rcurs = right_pt;
					}
					return outbatch;
				}
			}
		}
		lcurs = 0; //left part finished, reset leftcurs

	}
	rcurs = 0; //right part finished, reset rightcurs
	try {
		rightpart = (Batch) in_right.readObject();
		while (rightpart == null || rightpart.isEmpty()) {
			rightpart = (Batch) in_right.readObject();
		}

	} catch (EOFException e) {
		//end of right table partition
		eosr = true;
		try {
			in_right.close();
		} catch (IOException io) {
			io.printStackTrace();
			System.exit(1);
		}
		break;
	} catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
}
return outbatch;
}
```
When the join is complete, close() is called to remove the temporary files.
```java


	public boolean close(){
		//remove temp files
		for(int i = 0; i < numBuff - 1; i++){
			String right_f = "HJRight" + i + this.hashCode();
			String left_f = "HJLeft" + i + this.hashCode();
			File rf = new File(right_f);
			rf.delete();
			File lf = new File(left_f);
			lf.delete();
		}
		return true;

	}

}
```
## Distinct
### Distinct.java
The following code contains the Distinct Operator.
```java
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
```
When next() is called in Distinct, it reads in the next batch from its base operator as an input batch, it then creates a new batch, which would serve as the output batch. It then adds sequentially, each tuple from the input batch to the output batch. However, if there is already a tuple with the exact same data as the tuple to be added, it does not get added to the output batch. This ensures that there are no duplicates within batches.

Next, it calls open() for its base class. This causes the program to read in the original input from the start again. We create a comparison batch, which will then be used to compare against the output batch. We then keep calling next() for the base operator until we get to a batch that we have yet to consider as an input batch. This is done by keeping track of how many next() operations are called before obtaining the batch and this works as the batches that are created when the file is read are always in the same order. Once we obtain an appropriate comparison batch, we check if any of the tuples in the comparison batch have equal data to the tuples in the output batch, if there are, then we delete the tuple from the output batch. This carries on until we have finished reading in every batch. Finally, we call open() again and iterate through the batches until we reach our original input batch so that subsequent next() operations will obtain the correct batch.

```java
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
```
### parser.cup
The following are the parts of parser.cup that is modified to support Distinct. This allows the parser to read in queries that have the keyword DISTINCT and sets isDistinct to be true.
```java
terminal	SELECT,FROM,WHERE,GROUPBY,DISTINCT;

sqlquery ::= sqlquery:s  GROUPBY attlist:a
{:
s.setGroupByList(a);
parser.query=s;

:}
	| SELECT attlist:a FROM tablelist:t WHERE conditionlist:c
{:
	Vector v1= new Vector();
SQLQuery sq = new SQLQuery(a,t,c);
		parser.query=sq;
RESULT=sq;
:}
	| SELECT STAR FROM tablelist:t WHERE conditionlist:c
{:
		Vector a = new Vector();
		SQLQuery sq = new SQLQuery(a,t,c);
parser.query=sq;
RESULT=sq;
:}	
		| SELECT attlist:a FROM tablelist:t 
		{:
			Vector v1= new Vector();
		SQLQuery sq = new SQLQuery(a,t);
			parser.query=sq;
		RESULT=sq;
		:}
		| SELECT STAR FROM tablelist:t 
		{:
			Vector a = new Vector();
			SQLQuery sq = new SQLQuery(a,t);
		parser.query=sq;
		RESULT=sq;
		:}        | SELECT DISTINCT attlist:a FROM tablelist:t
					{:
						Vector v1= new Vector();
						SQLQuery sq = new SQLQuery(a,t);
						sq.setIsDistinct(true);
						parser.query=sq;
						RESULT=sq;
					:}| SELECT DISTINCT attlist:a FROM tablelist:t WHERE conditionlist:c
							{:
									Vector v1= new Vector();
									SQLQuery sq = new SQLQuery(a,t,c);
									sq.setIsDistinct(true);
									parser.query=sq;
									RESULT=sq;
							:}
;
```
### parser.java
The following are the parts of parser.java that is modified to support Distinct. These are similar to the non distinct variants, with the main difference being that it sets isDistinct to be true.
```java
 /*. . . . . . . . . . . . . . . . . . . .*/
case 7: // sqlquery ::= SELECT DISTINCT attlist FROM tablelist WHERE conditionlist 
{
	SQLQuery RESULT = null;
int aleft = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-4)).left;
int aright = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-4)).right;
Vector a = (Vector)((java_cup.runtime.Symbol) CUP$parser$stack.elementAt(CUP$parser$top-4)).value;
int tleft = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-2)).left;
int tright = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-2)).right;
Vector t = (Vector)((java_cup.runtime.Symbol) CUP$parser$stack.elementAt(CUP$parser$top-2)).value;
int cleft = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).left;
int cright = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).right;
Vector c = (Vector)((java_cup.runtime.Symbol) CUP$parser$stack.elementAt(CUP$parser$top-0)).value;

								Vector v1= new Vector();
								SQLQuery sq = new SQLQuery(a,t,c);
								sq.setIsDistinct(true);
								parser.query=sq;
								RESULT=sq;
						
	CUP$parser$result = new java_cup.runtime.Symbol(1/*sqlquery*/, ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-6)).left, ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).right, RESULT);
}
return CUP$parser$result;

/*. . . . . . . . . . . . . . . . . . . .*/
case 6: // sqlquery ::= SELECT DISTINCT attlist FROM tablelist 
{
	SQLQuery RESULT = null;
int aleft = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-2)).left;
int aright = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-2)).right;
Vector a = (Vector)((java_cup.runtime.Symbol) CUP$parser$stack.elementAt(CUP$parser$top-2)).value;
int tleft = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).left;
int tright = ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).right;
Vector t = (Vector)((java_cup.runtime.Symbol) CUP$parser$stack.elementAt(CUP$parser$top-0)).value;

					Vector v1= new Vector();
					SQLQuery sq = new SQLQuery(a,t);
					sq.setIsDistinct(true);
					parser.query=sq;
					RESULT=sq;
				
	CUP$parser$result = new java_cup.runtime.Symbol(1/*sqlquery*/, ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-4)).left, ((java_cup.runtime.Symbol)CUP$parser$stack.elementAt(CUP$parser$top-0)).right, RESULT);
}
return CUP$parser$result;

/*. . . . . . . . . . . . . . . . . . . .*/
```
### RandomInitialPlan.java
The following are the parts of RandomInitialPlan.java that is modified to support Distinct. The Distinct Operator would be called after the Project Operator in RandomInitialPlan, but only if isDistinct is true.

```java
public Operator prepareInitialPlan(){

tab_op_hash = new Hashtable();

createScanOp();
createSelectOp();
if(numJoin !=0){
	createJoinOp();
}
createProjectOp();
if (isdistinct) {
	createDistinctOp();
}
return root;
}
	
public void createDistinctOp() {
		Operator base = root;
		//If not distinct, do nothing
		if (isdistinct == false) {
			return;
		}
		else {
			root = new Distinct(base, OpType.DISTINCT);
			root.setSchema(base.getSchema());
		}
	}
```
### PlanCost.java
This following is the part of PlanCost.java that is modified to support Distinct. The cost was set to be tuples * tuples, as we would have to compare each tuple with each other tuple in the table to check for distinct tuples.
```java
 protected int calculateCost(Operator node){
if(node.getOpType()==OpType.JOIN){
	return getStatistics((Join)node);
}else if(node.getOpType() == OpType.SELECT){
	//System.out.println("PlanCost: line 40");
	return getStatistics((Select)node);
}else if(node.getOpType() == OpType.PROJECT){
	return getStatistics((Project)node);
}else if(node.getOpType() == OpType.SCAN){
	return getStatistics((Scan)node);
}else if(node.getOpType() == OpType.DISTINCT){
	return getStatistics((Distinct)node);

}
return -1;
}


protected int getStatistics(Distinct node) {
	int tuples=calculateCost(node.getBase());
	return tuples * tuples ;
}
```
### RandomOptimizer.java
The following is the code that is modified to support the Distinct Operator.
```java
protected Operator findNodeAt(Operator node,int joinNum){
	if(node.getOpType() == OpType.JOIN){
		if(((Join)node).getNodeIndex()==joinNum){
			return node;
		}else{
			Operator temp;
			temp= findNodeAt(((Join)node).getLeft(),joinNum);
			if(temp==null)
				temp = findNodeAt(((Join)node).getRight(),joinNum);
			return temp;
		}
	}else if(node.getOpType() == OpType.SCAN){
		return null;
	}else if(node.getOpType()==OpType.SELECT){
		//if sort/project/select operator
		return findNodeAt(((Select)node).getBase(),joinNum);
	}else if(node.getOpType()==OpType.PROJECT){
		return findNodeAt(((Project)node).getBase(),joinNum);
	}else if(node.getOpType()==OpType.DISTINCT){
		return findNodeAt(((Distinct)node).getBase(),joinNum);
	}else{
		return null;
	}
}
/** modifies the schema of operators which are modified due to selecing an alternative neighbor plan **/
private void modifySchema(Operator node){
	if(node.getOpType()==OpType.JOIN){
		Operator left = ((Join)node).getLeft();
		Operator right =((Join)node).getRight();
		modifySchema(left);
		modifySchema(right);
		node.setSchema(left.getSchema().joinWith(right.getSchema()));
	}else if(node.getOpType()==OpType.SELECT){
		Operator base= ((Select)node).getBase();
		modifySchema(base);
		node.setSchema(base.getSchema());
	}else if(node.getOpType()==OpType.PROJECT){
		Operator base = ((Project)node).getBase();
		modifySchema(base);
		Vector attrlist = ((Project)node).getProjAttr();
		node.setSchema(base.getSchema().subSchema(attrlist));
	}else if(node.getOpType()==OpType.DISTINCT){
		Operator base = ((Distinct)node).getBase();
		modifySchema(base);
		node.setSchema(base.getSchema());
	}
}



/** AFter finding a choice of method for each operator
	prepare an execution plan by replacing the methods with
	corresponding join operator implementation
	**/

public static Operator makeExecPlan(Operator node){

	if(node.getOpType()==OpType.JOIN){
		Operator left = makeExecPlan(((Join)node).getLeft());
		Operator right = makeExecPlan(((Join)node).getRight());
		int joinType = ((Join)node).getJoinType();
		//int joinType = JoinType.BLOCKNESTED;
		//int joinType = JoinType.NESTEDJOIN;
		int numbuff = BufferManager.getBuffersPerJoin();
		switch(joinType){
		case JoinType.NESTEDJOIN:

			NestedJoin nj = new NestedJoin((Join) node);
			nj.setLeft(left);
			nj.setRight(right);
			nj.setNumBuff(numbuff);
			return nj;

		/** Temporarity used simple nested join,
			replace with hasjoin, if implemented **/

		case JoinType.BLOCKNESTED:

			BlockNestedJoin bj = new BlockNestedJoin((Join) node);
			bj.setLeft(left);
			bj.setRight(right);
			bj.setNumBuff(numbuff);
			return bj;

		case JoinType.SORTMERGE:

			NestedJoin sm = new NestedJoin((Join) node);
			/* + other code */
			return sm;

		case JoinType.HASHJOIN:

			HashJoin hj = new HashJoin((Join) node);
			hj.setLeft(left);
			hj.setRight(right);
			hj.setNumBuff(numbuff);
			return hj;
		default:
			return node;
		}
	}else if(node.getOpType() == OpType.SELECT){
		Operator base = makeExecPlan(((Select)node).getBase());
		((Select)node).setBase(base);
		return node;
	}else if(node.getOpType() == OpType.PROJECT){
		Operator base = makeExecPlan(((Project)node).getBase());
		((Project)node).setBase(base);
		return node;
	} else if (node.getOpType() == OpType.DISTINCT) {
		Operator base = makeExecPlan(((Distinct) node).getBase());
		((Distinct) node).setBase(base);
		return node;
	}else{
		return node;
	}
}
```
## Simulated Annealing (2 Phase Optimization)
### RandomOptimizer.java

We referred to the provided paper “Randomized Algorithms for Optimising Large Join Queries”. The 2 Phase Optimization is carried out with the result of the original randomised optimisation algorithm. A random probability is introduced to allow uphill movements to neighbours, controlled by the variable ‘temperature’, such that we allow the algorithm to move out of the local minimum in search for a better plan. If a better plan cannot be found after some iterations (controlled by the variable ‘equilibrium’), it is said to have reached an equilibrium, and the stage ends. We reduce the temperature and start a new stage. This is repeated until temperature reaches zero. This is done to allow the algorithm to have another chance at picking a better plan if it was caught in a high cost local minimum.

```java
public Operator getTwoPhaseOptimizedPlan() {
	Operator currentStage = getOptimizedPlan();
	Operator globalMinimumStage = currentStage;
	PlanCost initCost = new PlanCost();
	int minCost = initCost.getCost(globalMinimumStage);
	double temperature = 0.1*minCost;
	int timeToFreeze = 4;
	System.out.println("Iterative Improvement Plan Cost: " + minCost);
	while (temperature > 1 && timeToFreeze > 0) {
		int equilibrium = 16*numJoin;
		while (equilibrium > 0) {
			Operator currentStageClone = currentStage.clone();
			Operator stageNeighbour = getNeighbor(currentStageClone);
			PlanCost pc = new PlanCost();
			PlanCost pc2 = new PlanCost();
			int deltaCost = pc.getCost(stageNeighbour) - pc2.getCost(currentStage);
			if (deltaCost <= 0) {
				currentStage = stageNeighbour;
			} else {
				SecureRandom secureRandom = new SecureRandom();
				double probability = Math.exp(-(deltaCost/temperature));
				if (secureRandom.nextDouble() < probability) {
					System.out.println("Jump to neighbour");
					currentStage = stageNeighbour;
				}
			}
			pc = new PlanCost();
			int competingStageCost = pc.getCost(currentStage);
			if (minCost > competingStageCost) {
				minCost = competingStageCost;
				globalMinimumStage = currentStage;
				timeToFreeze = 4;
			} else {
				timeToFreeze -= 1;
			}
			equilibrium -= 1;
		}
		temperature = temperature*0.95;
	}
	System.out.println("2 Phase Optimization Return Plan Cost: " + minCost);
	return globalMinimumStage;
}
```
### QueryMain.java
The original getOptimizedPlan() is replaced with the two phased optimization algorithm.
```java
public static void main(String[] args){
    //...
	RandomOptimizer ro = new RandomOptimizer(sqlquery);
	Operator logicalroot = ro.getTwoPhaseOptimizedPlan();
	//...
}
```
