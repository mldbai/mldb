/* -*- C++ -*- */

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

#ifndef SVDLIB_H
#define SVDLIB_H

#include <functional>

#define MAXLL 2

#define LMTNW   100000000 /* max. size of working area allowed  */


/******************************** Structures *********************************/
typedef struct smat *SMat;
typedef struct dmat *DMat;
typedef struct svdrec *SVDRec;

/* Harwell-Boeing sparse matrix. */
struct smat {
  long rows;
  long cols;
  long vals;     /* Total non-zero entries. */
  long *pointr;  /* For each col (plus 1), index of first non-zero entry. */
  long *rowind;  /* For each nz entry, the row index. */
  double *value; /* For each nz entry, the value. */
};

/* Row-major dense matrix.  Rows are consecutive vectors. */
struct dmat {
  long rows;
  long cols;
  double **value; /* Accessed by [row][col]. Free value[0] and value to free.*/
};

struct svdrec {
  int d;      /* Dimensionality (rank) */
  DMat Ut;    /* Transpose of left singular vectors. (d by m)
                 The vectors are the rows of Ut. */
  double *S;  /* Array of singular values. (length d) */
  DMat Vt;    /* Transpose of right singular vectors. (d by n)
                 The vectors are the rows of Vt. */
};


/******************************** Variables **********************************/

/* Version info */
extern const char *SVDVersion;

/* How verbose is the package: 0, 1 (default), 2 */
extern long SVDVerbosity;

/* Counter(s) used to track how much work is done in computing the SVD. */
enum svdCounters {SVD_MXV, SVD_COUNTERS};
extern long SVDCount[SVD_COUNTERS];
extern void svdResetCounters(void);

/******************************** Functions **********************************/

/* Creates an empty dense matrix. */
extern DMat svdNewDMat(std::size_t rows, std::size_t cols);
/* Frees a dense matrix. */
extern void svdFreeDMat(DMat D);

/* Creates an empty sparse matrix. */
SMat svdNewSMat(int rows, int cols, int vals);
/* Frees a sparse matrix. */
void svdFreeSMat(SMat S);

/* Creates an empty SVD record. */
SVDRec svdNewSVDRec(void);
/* Frees an svd rec and all its contents. */
void svdFreeSVDRec(SVDRec R);

/* Converts a sparse matrix to a dense one (without affecting former) */
DMat svdConvertStoD(SMat S);
/* Converts a dense matrix to a sparse one (without affecting former) */
SMat svdConvertDtoS(DMat D);

/* Transposes a dense matrix (returning a new one) */
DMat svdTransposeD(DMat D);
/* Transposes a sparse matrix (returning a new one) */
SMat svdTransposeS(SMat S);

enum storeVals {STORQ = 1, RETRQ, STORP, RETRP};

struct SVDParams {

    SVDParams();

    double **LanStore;
    long ierr;

    int nrows, ncols;  /// Size of the input matrix
    int nvals;  /// Number of non-zero values

    double eps1, reps, eps34;

    bool doU;  ///< Do we calculate the U vectors?

    void calcPrecision(int n);

    /**************************************************************
     * multiplication of matrix B by vector x, where B = A'A,     *
     * and A is nrow by ncol (nrow >> ncol). Hence, B is of order *
     * n = ncol (y stores product vector).		              *
     **************************************************************/
    std::function<void (double *, double *)> opb;

    /***********************************************************
     * multiplication of matrix A by vector x, where A is 	   *
     * nrow by ncol (nrow >> ncol).  y stores product vector.  *
     ***********************************************************/
    std::function<void (double *, double *)> opa;

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
    */
    std::function<void (long, long, long, double *)> store;

    void default_store(long n, long isw, long j, double *s);
};

/* Compatibility interface for svdlibc */
extern SVDRec svdLAS2(SMat A, long dimensions, long iterations, double end[2], 
                      double kappa);

/* Chooses default parameter values.  Set dimensions to 0 for all dimensions: */
extern SVDRec svdLAS2A(SMat A, long dimensions);
    
/* Performs the las2 SVD algorithm and returns the resulting Ut, S, and Vt. */
extern SVDRec svdLAS2(long dimensions, long iterations, double end[2], 
                      double kappa, SVDParams & params);

/* Chooses default parameter values.  Set dimensions to 0 for all dimensions: */
extern SVDRec svdLAS2A(long dimensions, SVDParams & params);

#endif /* SVDLIB_H */
