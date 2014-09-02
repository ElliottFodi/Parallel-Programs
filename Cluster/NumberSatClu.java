import edu.rit.pj2.Job;
import edu.rit.pj2.Rule;
import edu.rit.pj2.Task;
import edu.rit.pj2.Loop;
import edu.rit.pj2.LongVbl;
import edu.rit.pj2.Tuple;
import edu.rit.pj2.Node;
import java.io.File;
import edu.rit.pj2.TaskSpec;
import edu.rit.util.LongRange;
import java.util.Scanner;

/**
 * Class NumberSatClu is a parallel cluster program that determins the number
 * of satisfying assignments for a boolean expression. The program reads in a 
 * cnf formula from a file and tests every clause against all possable 
 * assignments. If all clauses result to satisfied for a given assignment 
 * then the boolean expresion has be satisfied and a counter is incremented.     
 * <P>
 * Usage: <TT>java pj2 NumberSatClu <I>file</I> </TT>
 * <BR><TT><I>file</I></TT> = CNF file
 * <P>
 * The program runs on a cluster in parallel utulizing mutilple threads. Reading 
 * from the file is done sequentially in the job process and the computation is 
 * done in parallel on multiple nodes if specified. The file is read in the job 
 * process and an array of clauses is sent to all the nodes by using a argument 
 * tuple. The job then starts K workertasks. Each clause in the file is inserted 
 * into an array of primitave type long. Each clause uses two indexs in the array. 
 * The first index holds the absolute value of every variable in the clause while 
 * the second holds only the absolute value of only the negative variables. If 
 * the clause contains no negative variables then the second index contains all 
 * zeros. Each thread then evaluates all clauses for one assignment. This is 
 * accomplised by taking the Symmetric difference of the value in second index 
 * with the assignment being tested. This result is then intersected with the 
 * first index to determin if the clause was satisfied or not. This is done for 
 * each clause in the expression. Once the expression has been tested for all 
 * assignments a reduction is performed to calculate how many times the expression 
 * was satisfied on that node. Then a result tuple is put into tuple space. Once 
 * all WorkerTasks have run a Finish rule is run. The finish rule takes all the 
 * result tuples in tuple space and adds the results and prints the answer.      
 *
 * @author  Elliott Fodi
 * @version 28-Sep-2013
 */

public class NumberSatClu extends Job {

	/**
	 * Class ClauseArgs is a Tuple containing the input arguments. 
	 * Sent from the job main program to all the worker tasks.
	 */
	private static class ClauseArgs extends Tuple {
		// Number of variables.
		public int variables;

		// Number of clauses.
		public int clauses;

		// Clause array; element is a bitset that holds the clauses
		// and two elements make up a clause
		public long[] clauseArray;

		// Construct a new ClauseArgs tuple.
		public ClauseArgs() {
		}

		// Construct a new ClauseArgs tuple with the given information.
		public ClauseArgs(int V, int C, long[] clauseArray) {
			this.variables = V;
			this.clauses = C;
			this.clauseArray = clauseArray;
		}
	}


	/**
	 * Class Result Tuple contains each task's results. Sent from each Worker Task.
	 */
	private static class ResultTuple extends Tuple {
		public long count;

		// Construct a new ResultTuple tuple with the given information.
		public ResultTuple(long count) {
			this.count = count;
		}
	}

	/**
	 * Class Chunk Tuple contains a range of iterations for a worker
    	 * task to compute.
	 */
	private static class ChunkTuple extends Tuple {
		public LongRange range;
		
		// Construct a new ClauseArgs tuple.
		public ChunkTuple() {
		}
		
		// Construct a new ChunkTuple tuple with the given information.
		public ChunkTuple(LongRange range) {
			this.range = range;
		}
	}

	/**
	 * Job main program.
	 */
	public void main(String[] args) throws Exception {
		// Parse command line arguments. Must have 2 arguments
		if (args.length < 2 || args.length > 2)
			usage();

		// get the number of workers
		int K = 1;
		if (args.length == 2)
			K = Integer.parseInt(args[1]);

		File file = new File(args[0]);

		// read in text from the cnf and check if text is correct
		Scanner scan = new Scanner(file);
		String format = scan.next();
		String formatCNF = scan.next();

		if (!format.equals("p") && !formatCNF.equals("cnf")) {
			System.out.println("error in file format");
			usage();
		}

		int variables;
		int clauses;
		long clauseArray[];

		// set variables
		variables = scan.nextInt();
		clauses = scan.nextInt();
		clauseArray = new long[clauses * 2];
		int temp;

		// read in the each clause and enter it into the array of primitive type
		// long
		for (int i = 0; i < clauses * 2; i += 2) {
			while (scan.hasNextInt()) {
				temp = scan.nextInt();
				if (temp < -63 || temp > 63) {
					System.out.println("Error unacceptable variable");
					usage();
				}
				if (temp != 0) {
					if (temp < 0) {
						temp = -temp;
						temp = temp - 1;
						clauseArray[i] = 1L << temp | clauseArray[i];
						clauseArray[i + 1] = 1L << temp | clauseArray[i + 1];
					} else {
						temp = temp - 1;
						clauseArray[i] = 1L << temp | clauseArray[i];
					}
				} else {
					break;
				}
			}
		}
		scan.close();

		// Put K copies of ClauseArgs tuple for worker tasks to take.
		for (int i = 1; i <= K; i++) {
			putTuple(new ClauseArgs(variables, clauses, clauseArray));
		}

		// Set up a task group with K workers.
		rule(new Rule().task(K, new TaskSpec(WorkerTask.class) .requires (new Node() .cores (Node.ALL_CORES))  ) );

		// Set up a task to reduce and print the answer.
		rule(new Rule().atFinish().task(new TaskSpec(ReduceTask.class) .args (""+K) .runInJobProcess(true)));

		long full = (1L << variables) - 1L;
		long A = 0L;
		int CF = 10;

		// Partition the iterations into chunks for the worker tasks.
		for (LongRange chunk : new LongRange(A, full).subranges(K * CF)) {
			putTuple(new ChunkTuple(chunk));
		}
	}

	/**
	 * Prints an error message and exits
	 */
	public void usage() {
		System.err.println("Usage: java pj2 NumberSatClu <file>");
		throw new IllegalArgumentException();
	}

	/*
	 * Class WorkerTask provides a task that computes chunks of
    	 * iterations to see if a clause is satisfied.
	 */
	public static class WorkerTask extends Task {

		LongVbl count;
		int variables;
		int clauses;
		long[] clauseArray;

		/**
		 * Reduce task main program.
		 */
		public void main(String[] args) throws Exception {

			// get the arguments from the ClauseArgs tuple
			ClauseArgs cg = (ClauseArgs) takeTuple(new ClauseArgs());

			this.variables = cg.variables;
			this.clauses = cg.clauses;
			this.clauseArray = cg.clauseArray;

			// Compute chunks of iterations.
			ChunkTuple template = new ChunkTuple();
			ChunkTuple chunk;

			count = new LongVbl.Sum(0);
			long lowerBound = 0l;
			long taskCount = 0;
			
			// try to take a chunk tuple if there isn't any exit
			while ((chunk = (ChunkTuple) tryToTakeTuple(template)) != null) {

				// test the expression against each assignment using the bounds 
				// from the chunk tuple 
				parallelFor(chunk.range.lb(), chunk.range.ub()).exec(new Loop() {

					LongVbl thrCount;
					int j;
					long b;
					long result;
					int counter;

					public void start() {
						thrCount = (LongVbl) threadLocal(count);
					}
					
					public void run(long i) {
						counter = 0;
						for (j = 0; j < clauseArray.length - 1; j += 2) {
							b = clauseArray[j + 1] ^ i;
							result = clauseArray[j] & b;
							if (result > 0) {
								counter++;
								b = 0L;
								result = 0L;
							} else {
								b = 0L;
								result = 0L;
								break;
							}
						}
						if (counter == clauses) {
							thrCount.item++;
						}
						counter = 0;
					}
				});

			// reset count.item to zero so more chunk tuples can be processed
			taskCount += count.item;
			count.item = 0;
				
			};

			// Report results.
			putTuple(new ResultTuple((long) taskCount));
		}
	}


	/**
    	 * Class TotientClu.ReduceTask combines the worker tasks' results and prints
    	 * the overall result.
	 */
	public static class ReduceTask extends Task {
		/**
		 * Reduce task main program.
		 */
		public void main(String[] args) throws Exception {

			int K = Integer.parseInt (args[0]);
			long count = 0L;

			// get all the result tuples and sum them
			for (int i = 0; i < K; i++){
            			count += ((ResultTuple) getTuple (i)).count;
			}
			
			// print results
			System.out.println(count);
		}
	}

}
