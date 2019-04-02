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

	/*
	 * Partition both the left and right table into their buckets separately
	 */

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
		}
		return outbatch;
	}


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