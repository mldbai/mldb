/*
Copyright © 2002, University of Tennessee Research Foundation.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

  Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Tennessee nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

#include <stdio.h>
#include <stdlib.h>
#include "svdlib.h"
#include "svdutil.h"

#include "mldb/jml/stats/distribution.h"
#include <iostream>
#include <string.h>


using namespace std;

const char *SVDVersion = "1.4";
long SVDVerbosity = 1;
long SVDCount[SVD_COUNTERS];

void svdResetCounters(void) {
  int i;
  for (i = 0; i < SVD_COUNTERS; i++)
    SVDCount[i] = 0;
}

/********************************* Allocation ********************************/

/* Row major order.  Rows are vectors that are consecutive in memory.  Matrix
   is initialized to empty. */
DMat svdNewDMat(size_t rows, size_t cols) {
  int i;
  DMat D = (DMat) malloc(sizeof(struct dmat));
  if (!D) {perror("svdNewDMat"); return NULL;}
  D->rows = rows;
  D->cols = cols;

  D->value = (double **) malloc(rows * sizeof(double *));
  if (!D->value) {
      cerr << "failed to allocate " << rows * sizeof(double *)
           << " bytes" << endl;
      SAFE_FREE(D);
      return NULL;
  }

  D->value[0] = (double *) calloc(rows * cols, sizeof(double));
  if (!D->value[0]) {
      cerr << "failed to allocate " << rows * cols * sizeof(double)
           << " bytes" << endl;
      SAFE_FREE(D->value);
      SAFE_FREE(D);
      return NULL;
  }

  for (i = 1; i < rows; i++) D->value[i] = D->value[i-1] + cols;
  return D;
}

void svdFreeDMat(DMat D) {
  if (!D) return;
  SAFE_FREE(D->value[0]);
  SAFE_FREE(D->value);
  free(D);
}


SMat svdNewSMat(int rows, int cols, int vals) {
  SMat S = (SMat) calloc(1, sizeof(struct smat));
  if (!S) {perror("svdNewSMat"); return NULL;}
  S->rows = rows;
  S->cols = cols;
  S->vals = vals;
  S->pointr = svd_longArray(cols + 1, true, "svdNewSMat: pointr");
  if (!S->pointr) {svdFreeSMat(S); return NULL;}
  S->rowind = svd_longArray(vals, false, "svdNewSMat: rowind");
  if (!S->rowind) {svdFreeSMat(S); return NULL;}
  S->value  = svd_doubleArray(vals, false, "svdNewSMat: value");
  if (!S->value)  {svdFreeSMat(S); return NULL;}
  return S;
}

void svdFreeSMat(SMat S) {
  if (!S) return;
  SAFE_FREE(S->pointr);
  SAFE_FREE(S->rowind);
  SAFE_FREE(S->value);
  free(S);
}


/* Creates an empty SVD record */
SVDRec svdNewSVDRec(void) {
  SVDRec R = (SVDRec) calloc(1, sizeof(struct svdrec));
  if (!R) {perror("svdNewSVDRec"); return NULL;}
  return R;
}

/* Frees an svd rec and all its contents. */
void svdFreeSVDRec(SVDRec R) {
  if (!R) return;
  if (R->Ut) svdFreeDMat(R->Ut);
  if (R->S) SAFE_FREE(R->S);
  if (R->Vt) svdFreeDMat(R->Vt);
  free(R);
}


/**************************** Conversion *************************************/

/* Converts a sparse matrix to a dense one (without affecting the former) */
DMat svdConvertStoD(SMat S) {
  int i, c;
  DMat D = svdNewDMat(S->rows, S->cols);
  if (!D) {
    svd_error("svdConvertStoD: failed to allocate D");
    return NULL;
  }
  for (i = 0, c = 0; i < S->vals; i++) {
    while (S->pointr[c + 1] <= i) c++;
    D->value[S->rowind[i]][c] = S->value[i];
  }
  return D;
}

/* Converts a dense matrix to a sparse one (without affecting the dense one) */
SMat svdConvertDtoS(DMat D) {
  SMat S;
  int i, j, n;
  for (i = 0, n = 0; i < D->rows; i++)
    for (j = 0; j < D->cols; j++)
      if (D->value[i][j] != 0) n++;
  
  S = svdNewSMat(D->rows, D->cols, n);
  if (!S) {
    svd_error("svdConvertDtoS: failed to allocate S");
    return NULL;
  }
  for (j = 0, n = 0; j < D->cols; j++) {
    S->pointr[j] = n;
    for (i = 0; i < D->rows; i++)
      if (D->value[i][j] != 0) {
        S->rowind[n] = i;
        S->value[n] = D->value[i][j];
        n++;
      }
  }
  S->pointr[S->cols] = S->vals;
  return S;
}

/* Transposes a dense matrix. */
DMat svdTransposeD(DMat D) {
  int r, c;
  DMat N = svdNewDMat(D->cols, D->rows);
  for (r = 0; r < D->rows; r++)
    for (c = 0; c < D->cols; c++)
      N->value[c][r] = D->value[r][c];
  return N;
}

/* Efficiently transposes a sparse matrix. */
SMat svdTransposeS(SMat S) {
  int r, c, i, j;
  SMat N = svdNewSMat(S->cols, S->rows, S->vals);
  /* Count number nz in each row. */
  for (i = 0; i < S->vals; i++)
    N->pointr[S->rowind[i]]++;
  /* Fill each cell with the starting point of the previous row. */
  N->pointr[S->rows] = S->vals - N->pointr[S->rows - 1];
  for (r = S->rows - 1; r > 0; r--)
    N->pointr[r] = N->pointr[r+1] - N->pointr[r-1];
  N->pointr[0] = 0;
  /* Assign the new columns and values. */
  for (c = 0, i = 0; c < S->cols; c++) {
    for (; i < S->pointr[c+1]; i++) {
      r = S->rowind[i];
      j = N->pointr[r+1]++;
      N->rowind[j] = c;
      N->value[j] = S->value[i];
    }
  }
  return N;
}

/***********************************************************
 * multiplication of matrix A by vector x, where A is 	   *
 * nrow by ncol (nrow >> ncol).  y stores product vector.  *
 ***********************************************************/
void svd_opa(SMat A, double *x, double *y) {
    long end, i, j;
    long *pointr = A->pointr, *rowind = A->rowind;
    double *value = A->value;

    //cerr << "svd_opa: x = " << distribution<float>(x, x + A->cols) << endl;
   
    SVDCount[SVD_MXV]++;
    memset(y, 0, A->rows * sizeof(double));
  
    for (i = 0; i < A->cols; i++) {
        end = pointr[i+1];
        for (j = pointr[i]; j < end; j++)
            y[rowind[j]] += value[j] * x[i]; 
    }
    return;
}

/**************************************************************
 * multiplication of matrix B by vector x, where B = A'A,     *
 * and A is nrow by ncol (nrow >> ncol). Hence, B is of order *
 * n = ncol (y stores product vector).		              *
 **************************************************************/
void svd_opb(SMat A, double *x, double *y) {
  long i, j, end;
  long *pointr = A->pointr, *rowind = A->rowind;
  double *value = A->value;
  long n = A->cols;

  double temp[A->rows];

  //cerr << "svd_opb: x = " << distribution<float>(x, x + A->cols) << endl;

  SVDCount[SVD_MXV] += 2;
  memset(y, 0, n * sizeof(double));
  for (i = 0; i < A->rows; i++) temp[i] = 0.0;
  
  for (i = 0; i < A->cols; i++) {
    end = pointr[i+1];
    for (j = pointr[i]; j < end; j++) 
      temp[rowind[j]] += value[j] * (*x); 
    x++;
  }
  for (i = 0; i < A->cols; i++) {
    end = pointr[i+1];
    for (j = pointr[i]; j < end; j++) 
      *y += value[j] * temp[rowind[j]];
    y++;
  }

  //cerr << "svd_opb: returned "
  //     << distribution<float>(y, y + A->cols) << endl;

  return;
}


static const char * const error_msg[] = {  /* error messages used by function    *
                               * check_parameters                   */
  NULL,
  "",
  "ENDL MUST BE LESS THAN ENDR",
  "REQUESTED DIMENSIONS CANNOT EXCEED NUM ITERATIONS",
  "ONE OF YOUR DIMENSIONS IS LESS THAN OR EQUAL TO ZERO",
  "NUM ITERATIONS (NUMBER OF LANCZOS STEPS) IS INVALID",
  "REQUESTED DIMENSIONS (NUMBER OF EIGENPAIRS DESIRED) IS INVALID",
  "6*N+4*ITERATIONS+1 + ITERATIONS*ITERATIONS CANNOT EXCEED NW",
  "6*N+4*ITERATIONS+1 CANNOT EXCEED NW", NULL};


/***********************************************************************
 *								       *
 *		      check_parameters()			       *
 *								       *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------
   Function validates input parameters and returns error code (long)  

   Parameters 
   ----------
  (input)
   dimensions   upper limit of desired number of eigenpairs of B           
   iterations   upper limit of desired number of lanczos steps             
   n        dimension of the eigenproblem for matrix B               
   endl     left end of interval containing unwanted eigenvalues of B
   endr     right end of interval containing unwanted eigenvalues of B
   vectors  1 indicates both eigenvalues and eigenvectors are wanted 
            and they can be found in lav2; 0 indicates eigenvalues only
   nnzero   number of nonzero elements in input matrix (matrix A)      
                                                                      
 ***********************************************************************/

long check_parameters(SMat A, long dimensions, long iterations, 
		      double endl, double endr, long vectors) {
   long error_index;
   error_index = 0;

   if (endl >/*=*/ endr)  error_index = 2;
   else if (dimensions > iterations) error_index = 3;
   else if (A->cols <= 0 || A->rows <= 0) error_index = 4;
   /*else if (n > A->cols || n > A->rows) error_index = 1;*/
   else if (iterations <= 0 || iterations > A->cols || iterations > A->rows)
     error_index = 5;
   else if (dimensions <= 0 || dimensions > iterations) error_index = 6;
   if (error_index) 
     svd_error("svdLAS2 parameter error: %s\n", error_msg[error_index]);
   return(error_index);
}


SVDParams::
SVDParams()
{
    store = std::bind(&SVDParams::default_store, this,
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3,
                      std::placeholders::_4);
    LanStore = 0;
    ierr = 0;
    nrows = ncols = nvals = 0;
    doU = true;
}


/***********************************************************************
 *                                                                     *
 *                     store()                                         *
 *                                                                     *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   store() is a user-supplied function which, based on the input
   operation flag, stores to or retrieves from memory a vector.


   Arguments 
   ---------

   (input)
   n       length of vector to be stored or retrieved
   isw     operation flag:
	     isw = 1 request to store j-th Lanczos vector q(j)
	     isw = 2 request to retrieve j-th Lanczos vector q(j)
	     isw = 3 request to store q(j) for j = 0 or 1
	     isw = 4 request to retrieve q(j) for j = 0 or 1
   s	   contains the vector to be stored for a "store" request 

   (output)
   s	   contains the vector retrieved for a "retrieve" request 

   Functions used
   --------------

   BLAS		svd_dcopy

 ***********************************************************************/

void
SVDParams::
default_store(long n, long isw, long j, double *s)
{
  /* printf("called store %ld %ld\n", isw, j); */
  switch(isw) {
  case STORQ:
    if (!LanStore[j + MAXLL]) {
      if (!(LanStore[j + MAXLL] = svd_doubleArray(n, false, "LanStore[j]")))
        svd_fatalError("svdLAS2: failed to allocate LanStore[%d]", j + MAXLL);
    }
    //cerr << "svd_store " << j << ": s = "
    //     << distribution<float>(s, s + n) << endl;
    svd_dcopy(n, s, 1, LanStore[j + MAXLL], 1);
    break;
  case RETRQ:	
    if (!LanStore[j + MAXLL])
      svd_fatalError("svdLAS2: store (RETRQ) called on index %d (not allocated)", 
                     j + MAXLL);
    svd_dcopy(n, LanStore[j + MAXLL], 1, s, 1);
    break;
  case STORP:	
    if (j >= MAXLL) {
      svd_error("svdLAS2: store (STORP) called with j >= MAXLL");
      break;
    }
    if (!LanStore[j]) {
      if (!(LanStore[j] = svd_doubleArray(n, false, "LanStore[j]")))
        svd_fatalError("svdLAS2: failed to allocate LanStore[%d]", j);
    }
    svd_dcopy(n, s, 1, LanStore[j], 1);
    break;
  case RETRP:	
    if (j >= MAXLL) {
      svd_error("svdLAS2: store (RETRP) called with j >= MAXLL");
      break;
    }
    if (!LanStore[j])
      svd_fatalError("svdLAS2: store (RETRP) called on index %d (not allocated)", 
                     j);
    svd_dcopy(n, LanStore[j], 1, s, 1);
    break;
  }
  return;
}

extern long ibeta, it, irnd, machep, negep;
extern double eps;

void
SVDParams::
calcPrecision(int n)
{
    eps1 = eps * sqrt((double) n);
    reps = sqrt(eps);
    eps34 = reps * sqrt(reps);
}

struct SMatSVDParams : public SVDParams {

    SMat mat;

    SMatSVDParams(SMat mat)
        : mat(mat)
    {
        opa = std::bind(&svd_opa, mat,
                        std::placeholders::_1, std::placeholders::_2);
        opb = std::bind(&svd_opb, mat,
                        std::placeholders::_1, std::placeholders::_2);
        
        ierr = 0;
        nrows = mat->rows;
        ncols = mat->cols;
        nvals = mat->vals;

        calcPrecision(mat->cols);
    }
};

SVDRec svdLAS2(SMat A, long dimensions, long iterations, double end[2], 
               double kappa)
{
    bool transpose = false;
    SVDRec R = NULL;
  
    svdResetCounters();

    int m = svd_imin(A->rows, A->cols);
    if (dimensions <= 0 || dimensions > m)
        dimensions = m;
    if (iterations <= 0 || iterations > m)
        iterations = m;
    if (iterations < dimensions) iterations = dimensions;

    /* Check parameters */
    if (check_parameters(A, dimensions, iterations, end[0], end[1], true))
        return NULL;

    /* If A is wide, the SVD is computed on its transpose for speed. */
    if (A->cols >= A->rows * 1.2) {
        if (SVDVerbosity > 0) printf("TRANSPOSING THE MATRIX FOR SPEED\n");
        transpose = true;
        A = svdTransposeS(A);
    }

    SMatSVDParams params(A);
    
    R = svdLAS2(dimensions, iterations, end, kappa, params);
    
    /* This swaps and transposes the singular matrices if A was transposed. */
    if (R && transpose) {
        DMat T;
        svdFreeSMat(A);
        T = R->Ut;
        R->Ut = R->Vt;
        R->Vt = T;
    }
    
    return R;
}

/* Chooses default parameter values.  Set dimensions to 0 for all dimensions: */
SVDRec svdLAS2A(SMat A, long dimensions)
{
  double end[2] = {-1.0e-30, 1.0e-30};
  double kappa = 1e-6;
  return svdLAS2(A, dimensions, 0, end, kappa);
}
