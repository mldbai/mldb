/*************************************************************************
                           (c) Copyright 2003
                              Douglas Rohde

                     adapted from SVDPACKC, which is

                           (c) Copyright 1993
                        University of Tennessee
                          All Rights Reserved                          
 *************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <fcntl.h>
#include "svdlib.h"
#include "svdutil.h"

#include "mldb/jml/stats/distribution.h"
#include <iostream>
#include "mldb/base/parallel.h"

using namespace std;


void   purge(long n, long ll, double *r, double *q, double *ra,  
             double *qa, double *wrk, double *eta, double *oldeta, long step, 
             double *rnmp, double tol, SVDParams & Params);
void   ortbnd(double *alf, double *eta, double *oldeta, double *bet, long step,
              double rnm, SVDParams & params);
double startv(double *wptr[], long step, long n, SVDParams & params);
//void   store(long, long, long, double *);
void   imtql2(long, long, double *, double *, double *, SVDParams & params);
void   imtqlb(long n, double d[], double e[], double bnd[],
              SVDParams & params);
void   write_header(long, long, double, double, long, double, long, long, 
                    long);
long   check_parameters(SMat A, long dimensions, long iterations, 
                        double endl, double endr, long vectors);
int    lanso(long iterations, long dimensions, double endl,
             double endr, double *ritz, double *bnd, double *wptr[], 
             long *neigp, long n, SVDParams & params);
long   ritvec(long n, SVDRec R, double kappa, double *ritz, 
              double *bnd, double *alf, double *bet, double *w2, 
              long steps, long neig, SVDParams & params);
long   lanczos_step(long first, long last, double *wptr[],
                    double *alf, double *eta, double *oldeta,
                    double *bet, long *ll, long *enough, double *rnmp, 
                    double *tolp, long n, SVDParams & params);
void   stpone(double *wrkptr[], double *rnmp, double *tolp, long n,
              SVDParams & params);
long   error_bound(long *, double, double, double *, double *, long step, 
                   double tol, SVDParams & params);

// Machine params; only need to be done once
long ibeta, it, irnd, machep, negep;
double eps;

void   machar(long *ibeta, long *it, long *irnd, long *machep, long *negep, double *eps);

struct AtInit {
    AtInit()
    {
        machar(&ibeta, &it, &irnd, &machep, &negep, &eps);
    }
} atInit;

/***********************************************************************
 *                                                                     *
 *                        main()                                       *
 * Sparse SVD(A) via Eigensystem of A'A symmetric Matrix 	       *
 *                  (double precision)                                 *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   This sample program uses landr to compute singular triplets of A via
   the equivalent symmetric eigenvalue problem                         

   B x = lambda x, where x' = (u',v'), lambda = sigma**2,
   where sigma is a singular value of A,
                                                                     
   B = A'A , and A is m (nrow) by n (ncol) (nrow >> ncol),                
                                                                 
   so that {u,sqrt(lambda),v} is a singular triplet of A.        
   (A' = transpose of A)                                      
                                                            
   User supplied routines: svd_opa, opb, store, timer              
                                                        
   svd_opa(     x,y) takes an n-vector x and returns A*x in y.
   svd_opb(ncol,x,y) takes an n-vector x and returns B*x in y.
                                                                  
   Based on operation flag isw, store(n,isw,j,s) stores/retrieves 
   to/from storage a vector of length n in s.                   
                                                               
   User should edit timer() with an appropriate call to an intrinsic
   timing routine that returns elapsed user time.                      


   External parameters 
   -------------------

   Defined and documented in las2.h


   Local parameters 
   ----------------

  (input)
   endl     left end of interval containing unwanted eigenvalues of B
   endr     right end of interval containing unwanted eigenvalues of B
   kappa    relative accuracy of ritz values acceptable as eigenvalues
              of B
	      vectors is not equal to 1
   r        work array
   n	    dimension of the eigenproblem for matrix B (ncol)
   dimensions   upper limit of desired number of singular triplets of A
   iterations   upper limit of desired number of Lanczos steps
   nnzero   number of nonzeros in A
   vectors  1 indicates both singular values and singular vectors are 
	      wanted and they can be found in output file lav2;
	      0 indicates only singular values are wanted 
   		
  (output)
   ritz	    array of ritz values
   bnd      array of error bounds
   d        array of singular values
   memory   total memory allocated in bytes to solve the B-eigenproblem


   Functions used
   --------------

   BLAS		svd_daxpy, svd_dscal, svd_ddot
   USER		svd_opa, svd_opb, timer
   MISC		write_header, check_parameters
   LAS2		landr


   Precision
   ---------

   All floating-point calculations are done in double precision;
   variables are declared as long and double.


   LAS2 development
   ----------------

   LAS2 is a C translation of the Fortran-77 LAS2 from the SVDPACK
   library written by Michael W. Berry, University of Tennessee,
   Dept. of Computer Science, 107 Ayres Hall, Knoxville, TN, 37996-1301

   31 Jan 1992:  Date written 

   Theresa H. Do
   University of Tennessee
   Dept. of Computer Science
   107 Ayres Hall
   Knoxville, TN, 37996-1301
   internet: tdo@cs.utk.edu

 ***********************************************************************/

/***********************************************************************
 *								       *
 *			  write_header()			       *
 *   Function writes out header of output file containing ritz values  *
 *								       *
 ***********************************************************************/

void write_header(long iterations, long dimensions, double endl, double endr, 
                  long vectors, double kappa, long nrow, long ncol, 
                  long vals) {
  printf("SOLVING THE [A^TA] EIGENPROBLEM\n");
  printf("NO. OF ROWS               = %6ld\n", nrow);
  printf("NO. OF COLUMNS            = %6ld\n", ncol);
  printf("NO. OF NON-ZERO VALUES    = %6ld\n", vals);
  printf("MATRIX DENSITY            = %6.2f%%\n", 
         ((float) vals / nrow) * 100 / ncol);
  /* printf("ORDER OF MATRIX A         = %5ld\n", n); */
  printf("MAX. NO. OF LANCZOS STEPS = %6ld\n", iterations);
  printf("MAX. NO. OF EIGENPAIRS    = %6ld\n", dimensions);
  printf("LEFT  END OF THE INTERVAL = %9.2E\n", endl);
  printf("RIGHT END OF THE INTERVAL = %9.2E\n", endr);
  printf("KAPPA                     = %9.2E\n", kappa);
  /* printf("WANT S-VECTORS?   [T/F]   =     %c\n", (vectors) ? 'T' : 'F'); */
  printf("\n");
  return;
}


/***********************************************************************
 *                                                                     *
 *				landr()				       *
 *        Lanczos algorithm with selective orthogonalization           *
 *                    Using Simon's Recurrence                         *
 *                       (double precision)                            *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   landr() is the LAS2 driver routine that, upon entry,
     (1)  checks for the validity of input parameters of the 
	  B-eigenproblem 
     (2)  determines several machine constants
     (3)  makes a Lanczos run
     (4)  calculates B-eigenvectors (singular vectors of A) if requested 
	  by user


   arguments
   ---------

   (input)
   n        dimension of the eigenproblem for A'A
   iterations   upper limit of desired number of Lanczos steps
   dimensions   upper limit of desired number of eigenpairs
   nnzero   number of nonzeros in matrix A
   endl     left end of interval containing unwanted eigenvalues of B
   endr     right end of interval containing unwanted eigenvalues of B
   vectors  1 indicates both eigenvalues and eigenvectors are wanted
              and they can be found in output file lav2; 
	    0 indicates only eigenvalues are wanted
   kappa    relative accuracy of ritz values acceptable as eigenvalues
	      of B (singular values of A)
   r        work array

   (output)
   j        number of Lanczos steps actually taken                     
   neig     number of ritz values stabilized                           
   ritz     array to hold the ritz values                              
   bnd      array to hold the error bounds


   External parameters
   -------------------

   Defined and documented in las2.h


   local parameters
   -------------------

   ibeta    radix for the floating-point representation
   it       number of base ibeta digits in the floating-point significand
   irnd     floating-point addition rounded or chopped
   machep   machine relative precision or round-off error
   negeps   largest negative integer
   wptr	    array of pointers each pointing to a work space


   Functions used
   --------------

   MISC         svd_dmax, machar, check_parameters
   LAS2         ritvec, lanso

 ***********************************************************************/

SVDRec svdLAS2A(long dimensions, SVDParams & params) {
  double end[2] = {-1.0e-30, 1.0e-30};
  double kappa = 1e-6;
  return svdLAS2(dimensions, 0, end, kappa, params);
}


SVDRec svdLAS2(long dimensions, long iterations, double end[2], 
               double kappa, SVDParams & params)
{
  long i, steps, neig, m;
  double *wptr[10], *ritz, *bnd;
  SVDRec R = NULL;
  params.ierr = 0; // reset the global error flag
  
  svdResetCounters();

  m = svd_imin(params.nrows, params.ncols);
  if (dimensions <= 0 || dimensions > m)
    dimensions = m;
  if (iterations <= 0 || iterations > m)
    iterations = m;
  if (iterations < dimensions) iterations = dimensions;

  /* Write output header */
  if (SVDVerbosity > 0)
    write_header(iterations, dimensions, end[0], end[1], true, kappa, params.nrows, 
                 params.ncols, params.nvals);

  int n = params.ncols;

  //cerr << "ibeta = " << ibeta << " it = " << it << " irnd = "
  //     << irnd << " machep = " << machep << " negep = " << negep
  //     << " eps1 = " << params.eps1 << " reps = " << params.reps << " eps34 = "
  //     << params.eps34 << " eps " << eps << endl;

  /* Allocate temporary space. */
  if (!(wptr[0] = svd_doubleArray(n, true, "las2: wptr[0]"))) goto abort;
  if (!(wptr[1] = svd_doubleArray(n, false, "las2: wptr[1]"))) goto abort;
  if (!(wptr[2] = svd_doubleArray(n, false, "las2: wptr[2]"))) goto abort;
  if (!(wptr[3] = svd_doubleArray(n, false, "las2: wptr[3]"))) goto abort;
  if (!(wptr[4] = svd_doubleArray(n, false, "las2: wptr[4]"))) goto abort;
  if (!(wptr[5] = svd_doubleArray(n, false, "las2: wptr[5]"))) goto abort;
  if (!(wptr[6] = svd_doubleArray(iterations, false, "las2: wptr[6]"))) 
    goto abort;
  if (!(wptr[7] = svd_doubleArray(iterations, false, "las2: wptr[7]"))) 
    goto abort;
  if (!(wptr[8] = svd_doubleArray(iterations, false, "las2: wptr[8]"))) 
    goto abort;
  if (!(wptr[9] = svd_doubleArray(iterations + 1, false, "las2: wptr[9]"))) 
    goto abort;
  /* Calloc may be unnecessary: */
  if (!(ritz    = svd_doubleArray(iterations + 1, true, "las2: ritz"))) 
    goto abort;  
  /* Calloc may be unnecessary: */
  if (!(bnd     = svd_doubleArray(iterations + 1, true, "las2: bnd"))) 
    goto abort;
  memset(bnd, 127, (iterations + 1) * sizeof(double));

  if (!(params.LanStore = (double **) calloc(iterations + MAXLL, sizeof(double *))))
    goto abort;

  /* Actually run the lanczos thing: */
  steps = lanso(iterations, dimensions, end[0], end[1], ritz, bnd, wptr, 
                &neig, n, params);

  /* Print some stuff. */
  if (SVDVerbosity > 0) {
    printf("NUMBER OF LANCZOS STEPS   = %6ld\n"
           "RITZ VALUES STABILIZED    = %6ld\n", steps + 1, neig);
  }
  if (SVDVerbosity > 2) {
    printf("\nCOMPUTED RITZ VALUES  (ERROR BNDS)\n");
    for (i = 0; i <= steps; i++)
      printf("%3ld  %22.14E  (%11.2E)\n", i + 1, ritz[i], bnd[i]);
  }

  SAFE_FREE(wptr[0]);
  SAFE_FREE(wptr[1]);
  SAFE_FREE(wptr[2]);
  SAFE_FREE(wptr[3]);
  SAFE_FREE(wptr[4]);
  SAFE_FREE(wptr[7]);
  SAFE_FREE(wptr[8]);

  /* Compute eigenvectors */
  kappa = svd_dmax(fabs(kappa), params.eps34);
  
  R = svdNewSVDRec();
  if (!R) {
    svd_error("svdLAS2: allocation of R failed");
    goto cleanup;
  }
  R->d  = /*svd_imin(nsig, dimensions)*/dimensions;

  if (params.doU) {
      R->Ut = svdNewDMat(R->d, params.nrows);
      
      if (!R->Ut) {
          svd_error("svdLAS2: allocation of R->Ut failed");
          goto cleanup;
      }
  }
  else R->Ut = 0;

  R->S  = svd_doubleArray(R->d, true, "las2: R->s");

  if (!R->S) {
    svd_error("svdLAS2: allocation of R->S failed");
    goto cleanup;
  }

  R->Vt = svdNewDMat(R->d, params.ncols);
  if (!R->Vt) {
    svd_error("svdLAS2: allocation of R failed");
    goto cleanup;
  }

  ritvec(n, R, kappa, ritz, bnd, wptr[6], wptr[9], wptr[5], steps, 
         neig, params);

  if (SVDVerbosity > 0)
      printf("SINGULAR VALUES FOUND     = %6d\n", R->d);

 cleanup:    
  for (i = 0; i <= 9; i++)
    SAFE_FREE(wptr[i]);
  SAFE_FREE(ritz);
  SAFE_FREE(bnd);
  if (params.LanStore) {
    for (i = 0; i < iterations + MAXLL; i++)
      SAFE_FREE(params.LanStore[i]);
    SAFE_FREE(params.LanStore);
  }

  return R;
abort:
  svd_error("svdLAS2: fatal error, aborting");
  return NULL;
}


/***********************************************************************
 *                                                                     *
 *                        ritvec()                                     *
 * 	    Function computes the singular vectors of matrix A	       *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   This function is invoked by landr() only if eigenvectors of the A'A
   eigenproblem are desired.  When called, ritvec() computes the 
   singular vectors of A and writes the result to an unformatted file.


   Parameters
   ----------

   (input)
   nrow       number of rows of A
   steps      number of Lanczos iterations performed
   fp_out2    pointer to unformatted output file
   n	      dimension of matrix A
   kappa      relative accuracy of ritz values acceptable as 
		eigenvalues of A'A
   ritz       array of ritz values
   bnd        array of error bounds
   alf        array of diagonal elements of the tridiagonal matrix T
   bet        array of off-diagonal elements of T
   w1, w2     work space

   (output)
   xv1        array of eigenvectors of A'A (right singular vectors of A)
   ierr	      error code
              0 for normal return from imtql2()
	      k if convergence did not occur for k-th eigenvalue in
	        imtql2()
   nsig       number of accepted ritz values based on kappa

   (local)
   s	      work array which is initialized to the identity matrix
	      of order (j + 1) upon calling imtql2().  After the call,
	      s contains the orthonormal eigenvectors of the symmetric 
	      tridiagonal matrix T

   Functions used
   --------------

   BLAS		svd_dscal, svd_dcopy, svd_daxpy
   USER		store
   		imtql2

 ***********************************************************************/

void rotateArray(double *a, int size, int x) {
  int i, j, n, start;
  double t1, t2;
  if (x == 0) return;
  j = start = 0;
  t1 = a[0];
  for (i = 0; i < size; i++) {
    n = (j >= x) ? j - x : j + size - x;
    t2 = a[n];
    a[n] = t1;
    t1 = t2;
    j = n;
    if (j == start) {
      start = ++j;
      t1 = a[j];
    }
  }
}

long ritvec(long n, SVDRec R, double kappa, double *ritz, double *bnd, 
            double *alf, double *bet, double *w2, long steps, long neig,
            SVDParams & params) {
  long js, jsq, i, k, /*size,*/ id2, tmp, nsig, x;
  double *s, *w1 = R->Vt->value[0];
  
  js = steps + 1;
  jsq = js * js;
  /*size = sizeof(double) * n;*/
  
  s = svd_doubleArray(jsq, true, "ritvec: s");
  
  /* initialize s to an identity matrix */
  for (i = 0; i < jsq; i+= (js+1)) s[i] = 1.0;
  svd_dcopy(js, alf, 1, w1, -1);
  svd_dcopy(steps, &bet[1], 1, &w2[1], -1);
  
  /* on return from imtql2(), w1 contains eigenvalues in ascending 
   * order and s contains the corresponding eigenvectors */
  imtql2(js, js, w1, w2, s, params);
  
  /*fwrite((char *)&n, sizeof(n), 1, fp_out2);
    fwrite((char *)&js, sizeof(js), 1, fp_out2);
    fwrite((char *)&kappa, sizeof(kappa), 1, fp_out2);*/
  /*id = 0;*/
  nsig = 0;
  if(params.ierr) {
    R->d = 0;
  }
  else {
      x = 0;
      id2 = jsq - js;
      for (k = 0; k < js; k++) {
        tmp = id2;
        if (bnd[k] <= kappa * fabs(ritz[k]) && k > js-neig-1) {
          if (--x < 0) x = R->d - 1;
          w1 = R->Vt->value[x];
          for (i = 0; i < n; i++) w1[i] = 0.0;
          for (i = 0; i < js; i++) {
            params.store(n, RETRQ, i, w2);
            svd_daxpy(n, s[tmp], w2, 1, w1, 1);
            tmp -= js;
          }
          /*fwrite((char *)w1, size, 1, fp_out2);*/
          
          /* store the w1 vector row-wise in array xv1;   
           * size of xv1 is (steps+1) * (nrow+ncol) elements 
           * and each vector, even though only ncol long,
           * will have (nrow+ncol) elements in xv1.      
           * It is as if xv1 is a 2-d array (steps+1) by     
           * (nrow+ncol) and each vector occupies a row  */

          /* j is the index in the R arrays, which are sorted by high to low 
             singular values. */
            
          /*for (i = 0; i < n; i++) R->Vt->value[x]xv1[id++] = w1[i];*/
          /*id += nrow;*/
          nsig++;
        }
        id2++;
      }

      /* Rotate the singular vectors and values. */
      /* x is now the location of the highest singular value. */
      rotateArray(R->Vt->value[0], R->Vt->rows * R->Vt->cols, 
                  x * R->Vt->cols);
      R->d = svd_imin(R->d, nsig);

      auto doOutput = [&] (int x)
          {
              /* multiply by matrix B first */
              double xv2[n];
              params.opb(R->Vt->value[x], xv2);
              double tmp0 = svd_ddot(n, R->Vt->value[x], 1, xv2, 1);
              svd_daxpy(n, -tmp0, R->Vt->value[x], 1, xv2, 1);
              tmp0 = sqrt(tmp0);
              double xnorm = sqrt(svd_ddot(n, xv2, 1, xv2, 1));
          
              /* multiply by matrix A to get (scaled) left s-vector */
              if (params.doU) params.opa(R->Vt->value[x], R->Ut->value[x]);
              double tmp1 = 1.0 / tmp0;
              if (params.doU) svd_dscal(params.nrows, tmp1, R->Ut->value[x], 1);
              xnorm *= tmp1;
              bnd[i] = xnorm;
              R->S[x] = tmp0;
          };

      MLDB::parallelMap(0, R->d, doOutput);

  }
  SAFE_FREE(s);

  return nsig;
}

/***********************************************************************
 *                                                                     *
 *                          lanso()                                    *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function determines when the restart of the Lanczos algorithm should 
   occur and when it should terminate.

   Arguments 
   ---------

   (input)
   n         dimension of the eigenproblem for matrix B
   iterations    upper limit of desired number of lanczos steps           
   dimensions    upper limit of desired number of eigenpairs             
   endl      left end of interval containing unwanted eigenvalues
   endr      right end of interval containing unwanted eigenvalues
   ritz      array to hold the ritz values                       
   bnd       array to hold the error bounds                          
   wptr      array of pointers that point to work space:            
  	       wptr[0]-wptr[5]  six vectors of length n		
  	       wptr[6] array to hold diagonal of the tridiagonal matrix T
  	       wptr[9] array to hold off-diagonal of T	
  	       wptr[7] orthogonality estimate of Lanczos vectors at 
		 step j
 	       wptr[8] orthogonality estimate of Lanczos vectors at 
		 step j-1

   (output)
   j         number of Lanczos steps actually taken
   neig      number of ritz values stabilized
   ritz      array to hold the ritz values
   bnd       array to hold the error bounds
   ierr      (globally declared) error flag
	     ierr = 8192 if stpone() fails to find a starting vector
	     ierr = k if convergence did not occur for k-th eigenvalue
		    in imtqlb()
	     ierr = 0 otherwise


   Functions used
   --------------

   LAS		stpone, error_bound, lanczos_step
   MISC		svd_dsort2
   UTILITY	svd_imin, svd_imax

 ***********************************************************************/

int lanso(long iterations, long dimensions, double endl,
          double endr, double *ritz, double *bnd, double *wptr[], 
          long *neigp, long n, SVDParams & params) {
  double *alf, *eta, *oldeta, *bet, *wrk, rnm, tol;
  long ll, first, last, ENOUGH, id2, id3, i, l, neig, j = 0, intro = 0;
  
  alf = wptr[6];
  eta = wptr[7];
  oldeta = wptr[8];
  bet = wptr[9];
  wrk = wptr[5];
  
  /* take the first step */
  stpone(wptr, &rnm, &tol, n, params);
  if (!rnm || params.ierr) return 0;
  eta[0] = params.eps1;
  oldeta[0] = params.eps1;
  ll = 0;
  first = 1;
  last = svd_imin(dimensions + svd_imax(8, dimensions), iterations);
  ENOUGH = false;
  /*id1 = 0;*/
  while (/*id1 < dimensions && */!ENOUGH) {
    if (rnm <= tol) rnm = 0.0;
    
    /* the actual lanczos loop */
    j = lanczos_step(first, last, wptr, alf, eta, oldeta, bet, &ll,
                     &ENOUGH, &rnm, &tol, n, params);
    if (ENOUGH) j = j - 1;
    else j = last - 1;
    first = j + 1;
    bet[j+1] = rnm;
    
    /* analyze T */
    l = 0;
    for (id2 = 0; id2 < j; id2++) {
      if (l > j) break;
      for (i = l; i <= j; i++) if (!bet[i+1]) break;
      if (i > j) i = j;
      
      /* now i is at the end of an unreduced submatrix */
      svd_dcopy(i-l+1, &alf[l],   1, &ritz[l],  -1);
      svd_dcopy(i-l,   &bet[l+1], 1, &wrk[l+1], -1);
      
      imtqlb(i-l+1, &ritz[l], &wrk[l], &bnd[l], params);
      
      if (params.ierr) {
        svd_error("svdLAS2: imtqlb failed to converge (ierr = %ld)\n", params.ierr);
        svd_error("  l = %ld  i = %ld\n", l, i);
        for (id3 = l; id3 <= i; id3++) 
          svd_error("  %ld  %lg  %lg  %lg\n", 
                    id3, ritz[id3], wrk[id3], bnd[id3]);
      }
      for (id3 = l; id3 <= i; id3++) 
        bnd[id3] = rnm * fabs(bnd[id3]);
      l = i + 1;
    }
    
    /* sort eigenvalues into increasing order */
    svd_dsort2((j+1) / 2, j + 1, ritz, bnd);

    /*    for (i = 0; i < iterations; i++)
      printf("%f ", ritz[i]);
      printf("\n"); */
    
    /* massage error bounds for very close ritz values */
    neig = error_bound(&ENOUGH, endl, endr, ritz, bnd, j, tol, params);
    *neigp = neig;
    
    /* should we stop? */
    if (neig < dimensions) {
      if (!neig) {
        last = first + 9;
        intro = first;
      } else last = first + svd_imax(3, 1 + ((j - intro) * (dimensions-neig)) /
                                     neig);
      last = svd_imin(last, iterations);
    } else ENOUGH = true;
    ENOUGH = ENOUGH || first >= iterations;
    /* id1++; */
    /* printf("id1=%d dimen=%d first=%d\n", id1, dimensions, first); */
  }
  params.store(n, STORQ, j, wptr[1]);
  return j;
}


/***********************************************************************
 *                                                                     *
 *			lanczos_step()                                 *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function embodies a single Lanczos step

   Arguments 
   ---------

   (input)
   n        dimension of the eigenproblem for matrix B
   first    start of index through loop				      
   last     end of index through loop				     
   wptr	    array of pointers pointing to work space		    
   alf	    array to hold diagonal of the tridiagonal matrix T
   eta      orthogonality estimate of Lanczos vectors at step j   
   oldeta   orthogonality estimate of Lanczos vectors at step j-1
   bet      array to hold off-diagonal of T                     
   ll       number of intitial Lanczos vectors in local orthog. 
              (has value of 0, 1 or 2)			
   enough   stop flag			

   Functions used
   --------------

   BLAS		svd_ddot, svd_dscal, svd_daxpy, svd_datx, svd_dcopy
   USER		store
   LAS		purge, ortbnd, startv
   UTILITY	svd_imin, svd_imax

 ***********************************************************************/

long lanczos_step(long first, long last, double *wptr[],
		  double *alf, double *eta, double *oldeta,
		  double *bet, long *ll, long *enough, double *rnmp, 
                  double *tolp, long n, SVDParams & params) {
   double t, *mid, rnm = *rnmp, tol = *tolp, anorm;
   long i, j;

   for (j=first; j<last; j++) {
      mid     = wptr[2];
      wptr[2] = wptr[1];
      wptr[1] = mid;
      mid     = wptr[3];
      wptr[3] = wptr[4];
      wptr[4] = mid;

      params.store(n, STORQ, j-1, wptr[2]);
      if (j-1 < MAXLL) params.store(n, STORP, j-1, wptr[4]);
      bet[j] = rnm;

      /* restart if invariant subspace is found */
      if (!bet[j]) {
          rnm = startv(wptr, j, n, params);
	 if (params.ierr) return j;
	 if (!rnm) *enough = true;
      }
      if (*enough) {
        /* added by Doug... */
        /* These lines fix a bug that occurs with low-rank matrices */
        mid     = wptr[2];
        wptr[2] = wptr[1];
        wptr[1] = mid;
        /* ...added by Doug */
        break;
      }

      /* take a lanczos step */
      t = 1.0 / rnm;
      svd_datx(n, t, wptr[0], 1, wptr[1], 1);
      svd_dscal(n, t, wptr[3], 1);
      params.opb(wptr[3], wptr[0]);
      svd_daxpy(n, -rnm, wptr[2], 1, wptr[0], 1);
      alf[j] = svd_ddot(n, wptr[0], 1, wptr[3], 1);
      svd_daxpy(n, -alf[j], wptr[1], 1, wptr[0], 1);

      /* orthogonalize against initial lanczos vectors */
      if (j <= MAXLL && (fabs(alf[j-1]) > 4.0 * fabs(alf[j])))
	 *ll = j;  
      for (i=0; i < svd_imin(*ll, j-1); i++) {
	 params.store(n, RETRP, i, wptr[5]);
	 t = svd_ddot(n, wptr[5], 1, wptr[0], 1);
	 params.store(n, RETRQ, i, wptr[5]);
         svd_daxpy(n, -t, wptr[5], 1, wptr[0], 1);
	 eta[i] = params.eps1;
	 oldeta[i] = params.eps1;
      }

      /* extended local reorthogonalization */
      t = svd_ddot(n, wptr[0], 1, wptr[4], 1);
      svd_daxpy(n, -t, wptr[2], 1, wptr[0], 1);
      if (bet[j] > 0.0) bet[j] = bet[j] + t;
      t = svd_ddot(n, wptr[0], 1, wptr[3], 1);
      svd_daxpy(n, -t, wptr[1], 1, wptr[0], 1);
      alf[j] = alf[j] + t;
      svd_dcopy(n, wptr[0], 1, wptr[4], 1);
      rnm = sqrt(svd_ddot(n, wptr[0], 1, wptr[4], 1));
      anorm = bet[j] + fabs(alf[j]) + rnm;
      tol = params.reps * anorm;

      /* update the orthogonality bounds */
      ortbnd(alf, eta, oldeta, bet, j, rnm, params);

      /* restore the orthogonality state when needed */
      purge(n, *ll, wptr[0], wptr[1], wptr[4], wptr[3], wptr[5], eta, oldeta,
            j, &rnm, tol, params);
      if (rnm <= tol) rnm = 0.0;
   }
   *rnmp = rnm;
   *tolp = tol;
   return j;
}

/***********************************************************************
 *                                                                     *
 *                          ortbnd()                                   *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Funtion updates the eta recurrence

   Arguments 
   ---------

   (input)
   alf      array to hold diagonal of the tridiagonal matrix T         
   eta      orthogonality estimate of Lanczos vectors at step j        
   oldeta   orthogonality estimate of Lanczos vectors at step j-1     
   bet      array to hold off-diagonal of T                          
   n        dimension of the eigenproblem for matrix B		    
   j        dimension of T					  
   rnm	    norm of the next residual vector			 
   eps1	    roundoff estimate for dot product of two unit vectors

   (output)
   eta      orthogonality estimate of Lanczos vectors at step j+1     
   oldeta   orthogonality estimate of Lanczos vectors at step j        


   Functions used
   --------------

   BLAS		svd_dswap

 ***********************************************************************/

void ortbnd(double *alf, double *eta, double *oldeta, double *bet, long step,
            double rnm, SVDParams & params) {
   long i;
   if (step < 1) return;
   if (rnm) {
      if (step > 1) {
	 oldeta[0] = (bet[1] * eta[1] + (alf[0]-alf[step]) * eta[0] -
		      bet[step] * oldeta[0]) / rnm + params.eps1;
      }
      for (i=1; i<=step-2; i++) 
	 oldeta[i] = (bet[i+1] * eta[i+1] + (alf[i]-alf[step]) * eta[i] +
		      bet[i] * eta[i-1] - bet[step] * oldeta[i])/rnm + params.eps1;
   }
   oldeta[step-1] = params.eps1;
   svd_dswap(step, oldeta, 1, eta, 1);  
   eta[step] = params.eps1;
   return;
}

/***********************************************************************
 *                                                                     *
 *				purge()                                *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function examines the state of orthogonality between the new Lanczos
   vector and the previous ones to decide whether re-orthogonalization 
   should be performed


   Arguments 
   ---------

   (input)
   n        dimension of the eigenproblem for matrix B		       
   ll       number of intitial Lanczos vectors in local orthog.       
   r        residual vector to become next Lanczos vector            
   q        current Lanczos vector			           
   ra       previous Lanczos vector
   qa       previous Lanczos vector
   wrk      temporary vector to hold the previous Lanczos vector
   eta      state of orthogonality between r and prev. Lanczos vectors 
   oldeta   state of orthogonality between q and prev. Lanczos vectors
   j        current Lanczos step				     

   (output)
   r	    residual vector orthogonalized against previous Lanczos 
	      vectors
   q        current Lanczos vector orthogonalized against previous ones


   Functions used
   --------------

   BLAS		svd_daxpy,  svd_dcopy,  svd_idamax,  svd_ddot
   USER		store

 ***********************************************************************/

void purge(long n, long ll, double *r, double *q, double *ra,  
	   double *qa, double *wrk, double *eta, double *oldeta, long step, 
           double *rnmp, double tol, SVDParams & params) {
  double t, tq, tr, reps1, rnm = *rnmp;
  long k, iteration, flag, i;
  
  if (step < ll+2) return; 
  
  k = svd_idamax(step - (ll+1), &eta[ll], 1) + ll;
  if (fabs(eta[k]) > params.reps) {
    reps1 = params.eps1 / params.reps;
    iteration = 0;
    flag = true;
    while (iteration < 2 && flag) {
      if (rnm > tol) {
        
        /* bring in a lanczos vector t and orthogonalize both 
         * r and q against it */
        tq = 0.0;
        tr = 0.0;
        for (i = ll; i < step; i++) {
          params.store(n,  RETRQ,  i,  wrk);
          t   = -svd_ddot(n, qa, 1, wrk, 1);
          tq += fabs(t);
          svd_daxpy(n,  t,  wrk,  1,  q,  1);
          t   = -svd_ddot(n, ra, 1, wrk, 1);
          tr += fabs(t);
          svd_daxpy(n, t, wrk, 1, r, 1);
        }
        svd_dcopy(n, q, 1, qa, 1);
        t   = -svd_ddot(n, r, 1, qa, 1);
        tr += fabs(t);
        svd_daxpy(n, t, q, 1, r, 1);
        svd_dcopy(n, r, 1, ra, 1);
        rnm = sqrt(svd_ddot(n, ra, 1, r, 1));
        if (tq <= reps1 && tr <= reps1 * rnm) flag = false;
      }
      iteration++;
    }
    for (i = ll; i <= step; i++) { 
      eta[i] = params.eps1;
      oldeta[i] = params.eps1;
    }
  }
  *rnmp = rnm;
  return;
}


/***********************************************************************
 *                                                                     *
 *                         stpone()                                    *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function performs the first step of the Lanczos algorithm.  It also
   does a step of extended local re-orthogonalization.

   Arguments 
   ---------

   (input)
   n      dimension of the eigenproblem for matrix B

   (output)
   ierr   error flag
   wptr   array of pointers that point to work space that contains
	    wptr[0]             r[j]
	    wptr[1]             q[j]
	    wptr[2]             q[j-1]
	    wptr[3]             p
	    wptr[4]             p[j-1]
	    wptr[6]             diagonal elements of matrix T 


   Functions used
   --------------

   BLAS		svd_daxpy, svd_datx, svd_dcopy, svd_ddot, svd_dscal
   USER		store, opb
   LAS		startv

 ***********************************************************************/

void stpone(double *wrkptr[], double *rnmp, double *tolp, long n,
            SVDParams & params) {
   double t, *alf, rnm, anorm;
   alf = wrkptr[6];

   /* get initial vector; default is random */
   rnm = startv(wrkptr, 0, n, params);
   if (rnm == 0.0 || params.ierr != 0) return;

   /* normalize starting vector */
   t = 1.0 / rnm;
   svd_datx(n, t, wrkptr[0], 1, wrkptr[1], 1);
   svd_dscal(n, t, wrkptr[3], 1);

   /* take the first step */
   params.opb(wrkptr[3], wrkptr[0]);
   alf[0] = svd_ddot(n, wrkptr[0], 1, wrkptr[3], 1);
   svd_daxpy(n, -alf[0], wrkptr[1], 1, wrkptr[0], 1);
   t = svd_ddot(n, wrkptr[0], 1, wrkptr[3], 1);
   svd_daxpy(n, -t, wrkptr[1], 1, wrkptr[0], 1);
   alf[0] += t;
   svd_dcopy(n, wrkptr[0], 1, wrkptr[4], 1);
   rnm = sqrt(svd_ddot(n, wrkptr[0], 1, wrkptr[4], 1));
   anorm = rnm + fabs(alf[0]);
   *rnmp = rnm;
   *tolp = params.reps * anorm;

   return;
}

/***********************************************************************
 *                                                                     *
 *                         startv()                                    *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function delivers a starting vector in r and returns |r|; it returns 
   zero if the range is spanned, and ierr is non-zero if no starting 
   vector within range of operator can be found.

   Parameters 
   ---------

   (input)
   n      dimension of the eigenproblem matrix B
   wptr   array of pointers that point to work space
   j      starting index for a Lanczos run
   eps    machine epsilon (relative precision)

   (output)
   wptr   array of pointers that point to work space that contains
	  r[j], q[j], q[j-1], p[j], p[j-1]
   ierr   error flag (nonzero if no starting vector can be found)

   Functions used
   --------------

   BLAS		svd_ddot, svd_dcopy, svd_daxpy
   USER		svd_opb, store
   MISC		random

 ***********************************************************************/

double startv(double *wptr[], long step, long n, SVDParams & params) {
   double rnm2, *r, t;
   long irand;
   long id, i;

   /* get initial vector; default is random */
   rnm2 = svd_ddot(n, wptr[0], 1, wptr[0], 1);
   irand = 918273 + step;
   r = wptr[0];
   for (id = 0; id < 3; id++) {
      if (id > 0 || step > 0 || rnm2 == 0) 
	 for (i = 0; i < n; i++) r[i] = svd_random2(&irand);
      svd_dcopy(n, wptr[0], 1, wptr[3], 1);

      /* apply operator to put r in range (essential if m singular) */
      params.opb(wptr[3], wptr[0]);
      svd_dcopy(n, wptr[0], 1, wptr[3], 1);
      rnm2 = svd_ddot(n, wptr[0], 1, wptr[3], 1);
      if (rnm2 > 0.0) break;
   }

   /* fatal error */
   if (rnm2 <= 0.0) {
      params.ierr = 8192;
      return(-1);
   }
   if (step > 0) {
      for (i = 0; i < step; i++) {
         params.store(n, RETRQ, i, wptr[5]);
	 t = -svd_ddot(n, wptr[3], 1, wptr[5], 1);
	 svd_daxpy(n, t, wptr[5], 1, wptr[0], 1);
      }

      /* make sure q[step] is orthogonal to q[step-1] */
      t = svd_ddot(n, wptr[4], 1, wptr[0], 1);
      svd_daxpy(n, -t, wptr[2], 1, wptr[0], 1);
      svd_dcopy(n, wptr[0], 1, wptr[3], 1);
      t = svd_ddot(n, wptr[3], 1, wptr[0], 1);
      if (t <= eps * rnm2) t = 0.0;
      rnm2 = t;
   }
   return(sqrt(rnm2));
}

/***********************************************************************
 *                                                                     *
 *			error_bound()                                  *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   Function massages error bounds for very close ritz values by placing 
   a gap between them.  The error bounds are then refined to reflect 
   this.


   Arguments 
   ---------

   (input)
   endl     left end of interval containing unwanted eigenvalues
   endr     right end of interval containing unwanted eigenvalues
   ritz     array to store the ritz values
   bnd      array to store the error bounds
   enough   stop flag


   Functions used
   --------------

   BLAS		svd_idamax
   UTILITY	svd_dmin

 ***********************************************************************/

long error_bound(long *enough, double endl, double endr, 
                 double *ritz, double *bnd, long step, double tol,
                 SVDParams & params) {
  long mid, i, neig;
  double gapl, gap;
  
  /* massage error bounds for very close ritz values */
  mid = svd_idamax(step + 1, bnd, 1);

  for (i=((step+1) + (step-1)) / 2; i >= mid + 1; i -= 1)
    if (fabs(ritz[i-1] - ritz[i]) < params.eps34 * fabs(ritz[i])) 
      if (bnd[i] > tol && bnd[i-1] > tol) {
        bnd[i-1] = sqrt(bnd[i] * bnd[i] + bnd[i-1] * bnd[i-1]);
        bnd[i] = 0.0;
      }
  
  
  for (i=((step+1) - (step-1)) / 2; i <= mid - 1; i +=1 ) 
    if (fabs(ritz[i+1] - ritz[i]) < params.eps34 * fabs(ritz[i])) 
      if (bnd[i] > tol && bnd[i+1] > tol) {
        bnd[i+1] = sqrt(bnd[i] * bnd[i] + bnd[i+1] * bnd[i+1]);
        bnd[i] = 0.0;
      }
  
  /* refine the error bounds */
  neig = 0;
  gapl = ritz[step] - ritz[0];
  for (i = 0; i <= step; i++) {
    gap = gapl;
    if (i < step) gapl = ritz[i+1] - ritz[i];
    gap = svd_dmin(gap, gapl);
    if (gap > bnd[i]) bnd[i] = bnd[i] * (bnd[i] / gap);
    if (bnd[i] <= 16.0 * eps * fabs(ritz[i])) {
      neig++;
      if (!*enough) *enough = endl < ritz[i] && ritz[i] < endr;
    }
  }   
  return neig;
}

/***********************************************************************
 *                                                                     *
 *				imtqlb()			       *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   imtqlb() is a translation of a Fortran version of the Algol
   procedure IMTQL1, Num. Math. 12, 377-383(1968) by Martin and 
   Wilkinson, as modified in Num. Math. 15, 450(1970) by Dubrulle.  
   Handbook for Auto. Comp., vol.II-Linear Algebra, 241-248(1971).  
   See also B. T. Smith et al, Eispack Guide, Lecture Notes in 
   Computer Science, Springer-Verlag, (1976).

   The function finds the eigenvalues of a symmetric tridiagonal
   matrix by the implicit QL method.


   Arguments 
   ---------

   (input)
   n      order of the symmetric tridiagonal matrix                   
   d      contains the diagonal elements of the input matrix           
   e      contains the subdiagonal elements of the input matrix in its
          last n-1 positions.  e[0] is arbitrary	             

   (output)
   d      contains the eigenvalues in ascending order.  if an error
            exit is made, the eigenvalues are correct and ordered for
            indices 0,1,...ierr, but may not be the smallest eigenvalues.
   e      has been destroyed.					    
   ierr   set to zero for normal return, j if the j-th eigenvalue has
            not been determined after 30 iterations.		    

   Functions used
   --------------

   UTILITY	svd_fsign
   MISC		svd_pythag

 ***********************************************************************/

void imtqlb(long n, double d[], double e[], double bnd[],
            SVDParams & params)

{
   long last, l, m, i, iteration;

   /* various flags */
   long exchange, convergence, underflow;	

   double b, test, g, r, s, c, p, f;

   if (n == 1) return;
   params.ierr = 0;
   bnd[0] = 1.0;
   last = n - 1;
   for (i = 1; i < n; i++) {
      bnd[i] = 0.0;
      e[i-1] = e[i];
   }
   e[last] = 0.0;
   for (l = 0; l < n; l++) {
      iteration = 0;
      while (iteration <= 30) {
	 for (m = l; m < n; m++) {
	    convergence = false;
	    if (m == last) break;
	    else {
	       test = fabs(d[m]) + fabs(d[m+1]);
	       if (test + fabs(e[m]) == test) convergence = true;
	    }
	    if (convergence) break;
	 }
	    p = d[l]; 
	    f = bnd[l]; 
	 if (m != l) {
	    if (iteration == 30) {
	       params.ierr = l;
	       return;
	    }
	    iteration += 1;
	    /*........ form shift ........*/
	    g = (d[l+1] - p) / (2.0 * e[l]);

            if (!isfinite(g))
                throw MLDB::Exception("NaN or infinite g value; probably you "
                                    "have asked for more singular values than "
                                    "the effective rank of the matrix");

            //cerr << "l = " << l << " n = " << n << " iteration = "
            //     << iteration
            //     << " g = " << g << " d[l + 1] = " << d[l + 1]
            //     << " p = " << p << " e[l] = " << e[l] << endl;

	    r = svd_pythag(g, 1.0);
	    g = d[m] - p + e[l] / (g + svd_fsign(r, g));
	    s = 1.0;
	    c = 1.0;
	    p = 0.0;
	    underflow = false;
	    i = m - 1;
	    while (underflow == false && i >= l) {
	       f = s * e[i];
	       b = c * e[i];
	       r = svd_pythag(f, g);
	       e[i+1] = r;
	       if (r == 0.0) underflow = true;
	       else {
		  s = f / r;
		  c = g / r;
		  g = d[i+1] - p;
		  r = (d[i] - g) * s + 2.0 * c * b;
		  p = s * r;
		  d[i+1] = g + p;
		  g = c * r - b;
		  f = bnd[i+1];
		  bnd[i+1] = s * bnd[i] + c * f;
		  bnd[i] = c * bnd[i] - s * f;
		  i--;
	       }
	    }       /* end while (underflow != false && i >= l) */
	    /*........ recover from underflow .........*/
	    if (underflow) {
	       d[i+1] -= p;
	       e[m] = 0.0;
	    }
	    else {
	       d[l] -= p;
	       e[l] = g;
	       e[m] = 0.0;
	    }
	 } 		       		   /* end if (m != l) */
	 else {

            /* order the eigenvalues */
	    exchange = true;
	    if (l != 0) {
	       i = l;
	       while (i >= 1 && exchange == true) {
	          if (p < d[i-1]) {
		     d[i] = d[i-1];
		     bnd[i] = bnd[i-1];
	             i--;
	          }
	          else exchange = false;
	       }
	    }
	    if (exchange) i = 0;
	    d[i] = p;
	    bnd[i] = f; 
	    iteration = 31;
	 }
      }			       /* end while (iteration <= 30) */
   }				   /* end for (l=0; l<n; l++) */
   return;
}						  /* end main */

/***********************************************************************
 *                                                                     *
 *				imtql2()			       *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   imtql2() is a translation of a Fortran version of the Algol
   procedure IMTQL2, Num. Math. 12, 377-383(1968) by Martin and 
   Wilkinson, as modified in Num. Math. 15, 450(1970) by Dubrulle.  
   Handbook for Auto. Comp., vol.II-Linear Algebra, 241-248(1971).  
   See also B. T. Smith et al, Eispack Guide, Lecture Notes in 
   Computer Science, Springer-Verlag, (1976).

   This function finds the eigenvalues and eigenvectors of a symmetric
   tridiagonal matrix by the implicit QL method.


   Arguments
   ---------

   (input)                                                             
   nm     row dimension of the symmetric tridiagonal matrix           
   n      order of the matrix                                        
   d      contains the diagonal elements of the input matrix        
   e      contains the subdiagonal elements of the input matrix in its
            last n-1 positions.  e[0] is arbitrary	             
   z      contains the identity matrix				    
                                                                   
   (output)                                                       
   d      contains the eigenvalues in ascending order.  if an error
            exit is made, the eigenvalues are correct but unordered for
            for indices 0,1,...,ierr.				   
   e      has been destroyed.					  
   z      contains orthonormal eigenvectors of the symmetric   
            tridiagonal (or full) matrix.  if an error exit is made,
            z contains the eigenvectors associated with the stored 
          eigenvalues.					
   ierr   set to zero for normal return, j if the j-th eigenvalue has
            not been determined after 30 iterations.		    


   Functions used
   --------------
   UTILITY	svd_fsign
   MISC		svd_pythag

 ***********************************************************************/

void imtql2(long nm, long n, double d[], double e[], double z[],
            SVDParams & params)

{
   long index, nnm, j, last, l, m, i, k, iteration, convergence, underflow;
   double b, test, g, r, s, c, p, f;
   if (n == 1) return;
   params.ierr = 0;
   last = n - 1;
   for (i = 1; i < n; i++) e[i-1] = e[i];
   e[last] = 0.0;
   nnm = n * nm;
   for (l = 0; l < n; l++) {
      iteration = 0;

      /* look for small sub-diagonal element */
      while (iteration <= 30) {
	 for (m = l; m < n; m++) {
	    convergence = false;
	    if (m == last) break;
	    else {
	       test = fabs(d[m]) + fabs(d[m+1]);
	       if (test + fabs(e[m]) == test) convergence = true;
	    }
	    if (convergence) break;
	 }
	 if (m != l) {

	    /* set error -- no convergence to an eigenvalue after
	     * 30 iterations. */     
	    if (iteration == 30) {
	       params.ierr = l;
	       return;
	    }
	    p = d[l]; 
	    iteration += 1;

	    /* form shift */
	    g = (d[l+1] - p) / (2.0 * e[l]);
	    r = svd_pythag(g, 1.0);
	    g = d[m] - p + e[l] / (g + svd_fsign(r, g));

            if (!isfinite(g))
                throw MLDB::Exception("NaN or infinite g value; probably you "
                                    "have asked for more singular values than "
                                    "the effective rank of the matrix");

	    s = 1.0;
	    c = 1.0;
	    p = 0.0;
	    underflow = false;
	    i = m - 1;
	    while (underflow == false && i >= l) {
	       f = s * e[i];
	       b = c * e[i];
	       r = svd_pythag(f, g);
	       e[i+1] = r;
	       if (r == 0.0) underflow = true;
	       else {
		  s = f / r;
		  c = g / r;
		  g = d[i+1] - p;
		  r = (d[i] - g) * s + 2.0 * c * b;
		  p = s * r;
		  d[i+1] = g + p;
		  g = c * r - b;

		  /* form vector */
		  for (k = 0; k < nnm; k += n) {
		     index = k + i;
		     f = z[index+1];
		     z[index+1] = s * z[index] + c * f;
		     z[index] = c * z[index] - s * f;
		  } 
		  i--;
	       }
	    }   /* end while (underflow != false && i >= l) */
	    /*........ recover from underflow .........*/
	    if (underflow) {
	       d[i+1] -= p;
	       e[m] = 0.0;
	    }
	    else {
	       d[l] -= p;
	       e[l] = g;
	       e[m] = 0.0;
	    }
	 }
	 else break;
      }		/*...... end while (iteration <= 30) .........*/
   }		/*...... end for (l=0; l<n; l++) .............*/

   /* order the eigenvalues */
   for (l = 1; l < n; l++) {
      i = l - 1;
      k = i;
      p = d[i];
      for (j = l; j < n; j++) {
	 if (d[j] < p) {
	    k = j;
	    p = d[j];
	 }
      }
      /* ...and corresponding eigenvectors */
      if (k != i) {
	 d[k] = d[i];
	 d[i] = p;
	  for (j = 0; j < nnm; j += n) {
	     p = z[j+i];
	     z[j+i] = z[j+k];
	     z[j+k] = p;
	  }
      }   
   }
   return;
}		/*...... end main ............................*/

/***********************************************************************
 *                                                                     *
 *				machar()			       *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   This function is a partial translation of a Fortran-77 subroutine 
   written by W. J. Cody of Argonne National Laboratory.
   It dynamically determines the listed machine parameters of the
   floating-point arithmetic.  According to the documentation of
   the Fortran code, "the determination of the first three uses an
   extension of an algorithm due to M. Malcolm, ACM 15 (1972), 
   pp. 949-951, incorporating some, but not all, of the improvements
   suggested by M. Gentleman and S. Marovich, CACM 17 (1974), 
   pp. 276-277."  The complete Fortran version of this translation is
   documented in W. J. Cody, "Machar: a Subroutine to Dynamically 
   Determine Determine Machine Parameters," TOMS 14, December, 1988.


   Parameters reported 
   -------------------

   ibeta     the radix for the floating-point representation       
   it        the number of base ibeta digits in the floating-point
               significand					 
   irnd      0 if floating-point addition chops		      
             1 if floating-point addition rounds, but not in the 
                 ieee style					
             2 if floating-point addition rounds in the ieee style
             3 if floating-point addition chops, and there is    
                 partial underflow				
             4 if floating-point addition rounds, but not in the
                 ieee style, and there is partial underflow    
             5 if floating-point addition rounds in the ieee style,
                 and there is partial underflow                   
   machep    the largest negative integer such that              
                 1.0+float(ibeta)**machep .ne. 1.0, except that 
                 machep is bounded below by  -(it+3)          
   negeps    the largest negative integer such that          
                 1.0-float(ibeta)**negeps .ne. 1.0, except that 
                 negeps is bounded below by  -(it+3)	       

 ***********************************************************************/

void machar(long *ibeta, long *it, long *irnd, long *machep, long *negep, double *eps) {

  volatile double beta, betain, betah, a, b, ZERO, ONE, TWO, temp, tempa,
    temp1;
  long i, itemp;
  
  ONE = (double) 1;
  TWO = ONE + ONE;
  ZERO = ONE - ONE;
  
  a = ONE;
  temp1 = ONE;
  while (temp1 - ONE == ZERO) {
    a = a + a;
    temp = a + ONE;
    temp1 = temp - a;
  }
  b = ONE;
  itemp = 0;
  while (itemp == 0) {
    b = b + b;
    temp = a + b;
    itemp = (long)(temp - a);
  }
  *ibeta = itemp;
  beta = (double) *ibeta;
  
  *it = 0;
  b = ONE;
  temp1 = ONE;
  while (temp1 - ONE == ZERO) {
    *it = *it + 1;
    b = b * beta;
    temp = b + ONE;
    temp1 = temp - b;
  }
  *irnd = 0; 
  betah = beta / TWO; 
  temp = a + betah;
  if (temp - a != ZERO) *irnd = 1;
  tempa = a + beta;
  temp = tempa + betah;
  if ((*irnd == 0) && (temp - tempa != ZERO)) *irnd = 2;
  
  *negep = *it + 3;
  betain = ONE / beta;
  a = ONE;
  for (i = 0; i < *negep; i++) a = a * betain;
  b = a;
  temp = ONE - a;
  while (temp-ONE == ZERO) {
    a = a * beta;
    *negep = *negep - 1;
    temp = ONE - a;
  }
  *negep = -(*negep);
  
  *machep = -(*it) - 3;
  a = b;
  temp = ONE + a;
  while (temp - ONE == ZERO) {
    a = a * beta;
    *machep = *machep + 1;
    temp = ONE + a;
  }
  *eps = a;
  return;
}

