
import edu.rit.pj2.Task;
import edu.rit.pj2.Loop;
import edu.rit.pj2.LongVbl;
import edu.rit.pj2.LongParallelForLoop;
import java.io.File;
import java.util.Scanner;

/**
 * Class NumberSatSmp is a parallel program that determins the number
 * of satisfying assignments for a boolean expression. The program reads in a 
 * cnf formula from a file and tests every clause against all possable 
 * assignments. If all clauses result to satisfied for a given assignment 
 * then the boolean expresion has be satisfied and a counter is incremented.     
 * <P>
 * Usage: <TT>java pj2 NumberSatSmp <I>file</I> </TT>
 * <BR><TT><I>file</I></TT> = CNF file
 * <P>
 * The program runs in parallel utulizing mutilple threads. Reading from the 
 * file is done sequentially and the computation is done in parallel. Each 
 * clause in the file is inserted into an array of primitave type long. Each 
 * clause uses two indexs in the array. The first index holds the absolute 
 * value of every variable in the clause while the second holds only the 
 * absolute value of only the negative variables. If the clause contains no 
 * negative variables then the second index contains all zeros.
 * Each thread then evaluates all clauses for one assignment. This is accomplised 
 * by taking the Symmetric difference of the value in second index with the 
 * assignment being tested. This result is then intersected with the first index 
 * to determin if the clause was satisfied or not. This is done for each clause 
 * in the expression. Once the expression has been tested for all assignments a 
 * reduction is performed to calculate how many times the expression was satisfied.      
 *
 * @author  Elliott Fodi
 * @version 28-Sep-2013
 */

public class NumberSatSmp extends Task 
{
	// declare variables
	LongVbl count;
	int variables;
	int clauses;
	long clauseArray[];

   /**
    * Main program.
    */
	public void main(String[] args)  throws Exception {

		// Parse command line arguments.
	    	if (args.length != 1) usage();
	    	
		File file = new File (args[0]);
	
		//read in text from the cnf and check if text is correct
		Scanner scan = new Scanner (file);
		String format = scan.next();
		String formatCNF = scan.next();
	 
		if(!format.equals("p") && !formatCNF.equals("cnf")){
			System.out.println("error in file format");
			usage();
		}
	
		// set variables
		variables = scan.nextInt();
		clauses = scan.nextInt();
		clauseArray = new long[clauses * 2];
		int temp;

		// read in the each clause and enter it into the array of primitiave type long 			
		for(int i = 0; i < clauses * 2; i+=2){
			while(scan.hasNextInt()){		
				temp = scan.nextInt();
				if(temp < -63 || temp > 63){
				    System.out.println("Error inacceptable variable");
				    usage();
				}	
				if(temp != 0 ){	
				    if (temp < 0){
					temp = -temp;
					temp = temp - 1;
					clauseArray[i] = 1L << temp | clauseArray[i];
					clauseArray[i + 1] = 1L << temp | clauseArray[i + 1];
				    }else{ 
					temp = temp - 1;
					clauseArray[i] = 1L << temp | clauseArray[i];
				    }
				}else{
				    break;
				}
			}
		}

		// declare and initialize variables for use in parallelFor loop
		long A;
		long full;

		full = (1L << variables) - 1L;
		A = 0L;
		count = new LongVbl.Sum (0);
		
		// test the expression against each assignment 	
		parallelFor ( A, full ) .exec (new Loop() 
		{
			LongVbl thrCount;
			int j;	
			long b;
			long result;
			int counter;				

			public void start(){
				thrCount = (LongVbl) threadLocal (count);	
			}

			public void run(long i){
				counter = 0;
				 
				for ( j = 0; j < clauseArray.length - 1; j += 2){
					b = clauseArray[j+1] ^ i;
					result = clauseArray[j] & b;
					if (result > 0){
					    counter++;
					    b = 0L;
					    result = 0L;
					}else{  
					    b = 0L;
					    result = 0L;
					    break; 
					}
				} 
				if (counter == clauses){
					thrCount.item++;
				}
				counter = 0;
			}
		});
		System.out.println(count.item);	
	}
	

   	/**
    	* Prints an error message and exits
    	*/
	public void usage(){
		System.err.println ("Usage: java pj2 NumberSatSmp <file>");
		throw new IllegalArgumentException();
	}

}

