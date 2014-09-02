/**
 * Program Diversity index computes an approximation of pi in parallel on the GPU by
 * generating N random (x,y) points in the unit square and counting how many
 * fall within a distance of 1 from the origin.
 *
 * Usage: PiGPU <seed> <trials> <pop1> <pop2> ...
 * <seed> = Pseudorandom number generator seed
 * <trials> = Number of trials, trials >= 1
 * <pop1> = population in group 1, pop1 >= 1 
 */

#include <stdlib.h>
#include <stdio.h>
#include <cuda_runtime_api.h>

#include "Util.cu"
#include "Random.cu"

//------------------------------------------------------------------------------
// DEVICE FUNCTIONS

// Number of threads per block.
#define NT 1024

// Overall counter variable in global memory.
__device__ unsigned long long int devCount;

// Per-thread counter variables in shared memory.
__shared__ unsigned long long int shrCount [NT];

/**
 * Device kernel to compute Diversity index.
 *
 * Called with a one-dimensional grid of one-dimensional blocks, NB blocks, NT
 * threads per block.
 *
 * @param  seed  Pseudorandom number generator seed.
 * @param  trials     Number of trials.
 * @param  numberOfArgs  Number of populations   
 * @param  total      total number of people
 * @param  dev_sectionsSP   Starting range for all sections
 * @param  dev_sectionsEP   Ending range for all sections
 */
__global__ void computeDiversityIndex
	(unsigned long long int seed, 
	 unsigned long long int trials, 
	 int numberOfArgs, 
	 int total,
	 int *dev_sectionsSP,
	 int *dev_sectionsEP){
   
   // Declare variables in kernel
   int x, size, rank;
   int catigory1 = 0;
   int catigory2 = 0;
   unsigned long long int thrTrialsSize, lb, ub, count;
   int random_person1 = 0;
   int random_person2 = 0;

   // Pseudo random number generator
   prng_t prng;

   // Determine number of threads and this thread's rank.
   x = threadIdx.x;
   size = gridDim.x*NT;
   rank = blockIdx.x*NT + x;

   // Determine iterations for this thread.
   thrTrialsSize = (trials + size - 1)/size;
   lb = rank*thrTrialsSize;
   ub = min (lb + thrTrialsSize, trials) - 1;

   // Initialize per-thread prng and count.
   prngSetSeed (&prng, seed + rank);
   count = 0;

	  // Pick random people and test if they are in the same population
	  for(int t = lb; t <= ub; t++){

		random_person1 = prngNextInt(&prng, total )+ 1 ;
		random_person2 = prngNextInt(&prng, total )+ 1 ;

		while(random_person2 == random_person1){
			random_person1 = prngNextInt(&prng, total )+ 1 ;
			random_person2 = prngNextInt(&prng, total )+ 1 ;
		}

		for(int i = 0; i < numberOfArgs; i++){
			
			if(random_person1 >= dev_sectionsSP[i] && random_person1 <= dev_sectionsEP[i]){
				catigory1 = i;
			}
			
			if(random_person2 >= dev_sectionsSP[i] && random_person2 <= dev_sectionsEP[i]){
				catigory2 = i;
			}
		}

		if(catigory1 != catigory2){
			count++;
		}

		catigory1 = 0;
		catigory2 = 0;
	  }
	  

   // Shared memory parallel reduction within thread block.
   shrCount[x] = count;
   __syncthreads();
   for (int i = NT/2; i > 0; i >>= 1){
      if (x < i){
         shrCount[x] += shrCount[x+i];
		 }
      __syncthreads();
      }

   // Atomic reduction into overall counter.
   if (x == 0)
      atomicAdd (&devCount, shrCount[0]);
   }

//------------------------------------------------------------------------------
// HOST FUNCTIONS

/**
 * Print a usage message and exit.
 */
static void usage()
   {
   fprintf (stderr, "Usage: DiversityIndex <seed> <trials> <pop1> <pop2> ...\n");
   fprintf (stderr, "<seed> = Pseudorandom number generator seed\n");
   fprintf (stderr, "<trials> = Number of trials, N >= 1\n");
   fprintf (stderr, "<pop> = population count, pop >= 1\n");
   exit (1);
   }

/**
 * Main program.
 */
int main(int argc, char *argv[]){

   // declare variables
   unsigned long long int seed, trials, hostCount;
   int dev, NB;
   int numberOfArgs = 0;
   
   // Minus 3 because 0 is the program name, 1 is the seed, 2 is the trials
   numberOfArgs = argc - 3;
   int arguments[numberOfArgs];
   int temp;
   
   int sectionsSP[numberOfArgs];
   int *dev_sectionsSP;
   size_t SPbytes = numberOfArgs * sizeof(int);
   
   
   int sectionsEP[numberOfArgs];
   int *dev_sectionsEP;
   size_t EPbytes = numberOfArgs * sizeof(int);
   
   int total = 0;
   
   
   // Parse command line arguments.
   if (argc < 4) usage();
   progname = argv[0];
   if (sscanf (argv[1], "%llu", &seed) != 1) usage();
   if (sscanf (argv[2], "%llu", &trials) != 1 || trials < 1) usage();
   
    for(int i = 0; i < numberOfArgs; i++){
		if (sscanf (argv[i+3], "%d", &temp) != 1) usage();
		arguments[i] = temp;
    }
	
	// If one <pop> and 1 person in that population print and exit
	if(numberOfArgs == 1 && arguments[0] == 1){
		printf ("Diversity index = 0/0 = 0.0\n");
		return 0;
	}
   
   
   
    // calculate the ranges for each population
    sectionsSP[0] = 1;
	sectionsEP[0] = arguments[0];
	total = arguments[0];
	
   	for(int i = 1; i < numberOfArgs; i++ ){
		sectionsSP[i] = sectionsEP[i -1] + 1;
		total = sectionsEP[i] = (sectionsEP[i- 1] ) + arguments[i];
	}
	

   // Set CUDA device and determine number of multiprocessors (thread blocks).
   dev = setCudaDevice();
   checkCuda
      (cudaDeviceGetAttribute (&NB, cudaDevAttrMultiProcessorCount, dev),
       "Cannot get number of multiprocessors");

   // Initialize overall counter.
   hostCount = 0;
   
    // allocate memory on card for dev_sections arrays
    checkCuda (cudaMalloc (&dev_sectionsSP, SPbytes), 
	  "Cannot allocate dev_sectionsSP");
	  
	checkCuda (cudaMalloc (&dev_sectionsEP, EPbytes),
      "Cannot allocate dev_sectionsEP");
   
	// Copy sections SP and EP arrays to device.
    checkCuda (cudaMemcpy (dev_sectionsSP, sectionsSP, SPbytes, cudaMemcpyHostToDevice),
      "Cannot upload dev_sectionsSP");
	  
    checkCuda (cudaMemcpy (dev_sectionsEP, sectionsEP, EPbytes, cudaMemcpyHostToDevice),
      "Cannot upload dev_sectionsEP");
   
	// Copy host count to device count 
    checkCuda(cudaMemcpyToSymbol (devCount, &hostCount, sizeof(hostCount)),
      "Cannot initialize devCount");

	   
   // Compute Diversity index in parallel on the GPU. 
   computeDiversityIndex <<< NB, NT >>> (seed, trials, numberOfArgs, total, dev_sectionsSP, dev_sectionsEP);
   cudaThreadSynchronize();
   checkCuda
      (cudaGetLastError(),
       "Cannot launch computeDiversityIndex() kernel");

   // Get overall counter from GPU.
   checkCuda
      (cudaMemcpyFromSymbol (&hostCount, devCount, sizeof(hostCount)),
       "Cannot copy devCount to hostCount");
	
	
   // Print results.
   printf ("Diversity index = %llu/%llu = %.1f\n", hostCount, trials, 100.0*hostCount/trials);
   }
