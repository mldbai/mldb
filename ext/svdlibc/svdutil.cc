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
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include "svdlib.h"
#include "svdutil.h"
#include <string>
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include "mldb/base/exc_assert.h"


using namespace std;
using namespace MLDB;


long *svd_longArray(long size, char empty, const char *name) {
  long *a;
  if (empty) a = (long *) calloc(size, sizeof(long));
  else a = (long *) malloc(size * sizeof(long));
  if (!a) {
    perror(name);
    /* exit(errno); */
  }
  return a;
}

double *svd_doubleArray(long size, char empty, const char *name) {
  double *a;
  if (empty) a = (double *) calloc(size, sizeof(double));
  else a = (double *) malloc(size * sizeof(double));
  if (!a) {
    perror(name);
    /* exit(errno); */
  }
  return a;
}

void svd_debug(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
}

void svd_error(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);

  string message = MLDB::vformat(fmt, args);
  va_end(args);
  throw MLDB::Exception("SVD Error: " + message);
}

void svd_fatalError(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);

  string message = vformat(fmt, args);
  va_end(args);
  throw MLDB::Exception("SVD Fatal Error: " + message);
}

/************************************************************** 
 * Function scales a vector by a constant.     		      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
void svd_dscal(long n, double da, double *dx, long incx) {
  long i;
  
  if (n <= 0 || incx == 0) return;
  if (incx < 0) dx += (-n+1) * incx;
  for (i=0; i < n; i++) {
    *dx *= da;
    dx += incx;
  }
  return;
}

/************************************************************** 
 * function scales a vector by a constant.	     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
void svd_datx(long n, double da, double *dx, long incx, double *dy, long incy) {
  long i;
  
  if (n <= 0 || incx == 0 || incy == 0 || da == 0.0) return;
  if (incx == 1 && incy == 1) 
    for (i=0; i < n; i++) *dy++ = da * (*dx++);
  
  else {
    if (incx < 0) dx += (-n+1) * incx;
    if (incy < 0) dy += (-n+1) * incy;
    for (i=0; i < n; i++) {
      *dy = da * (*dx);
      dx += incx;
      dy += incy;
    }
  }
  return;
}

/************************************************************** 
 * Function copies a vector x to a vector y	     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
void svd_dcopy(long n, double *dx, long incx, double *dy, long incy) {
  long i;
  
  if (n <= 0 || incx == 0 || incy == 0) return;
  if (incx == 1 && incy == 1) 
    for (i=0; i < n; i++) *dy++ = *dx++;
  
  else {
    if (incx < 0) dx += (-n+1) * incx;
    if (incy < 0) dy += (-n+1) * incy;
    for (i=0; i < n; i++) {
      *dy = *dx;
      dx += incx;
      dy += incy;
    }
  }
  return;
}

/************************************************************** 
 * Function forms the dot product of two vectors.      	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
double svd_ddot(long n, double *dx, long incx, double *dy, long incy) {
  long i;
  double dot_product;
  
  if (n <= 0 || incx == 0 || incy == 0) return(0.0);
  dot_product = 0.0;
  if (incx == 1 && incy == 1) 
    for (i=0; i < n; i++) dot_product += (*dx++) * (*dy++);
  else {
    if (incx < 0) dx += (-n+1) * incx;
    if (incy < 0) dy += (-n+1) * incy;
    for (i=0; i < n; i++) {
      dot_product += (*dx) * (*dy);
      dx += incx;
      dy += incy;
      }
  }
  return(dot_product);
}

/************************************************************** 
 * Constant times a vector plus a vector     		      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
void svd_daxpy (long n, double da, double *dx, long incx, double *dy, long incy) {
  long i;
  
  if (n <= 0 || incx == 0 || incy == 0 || da == 0.0) return;
  if (incx == 1 && incy == 1) 
    for (i=0; i < n; i++) {
      *dy += da * (*dx++);
      dy++;
    }
  else {
    if (incx < 0) dx += (-n+1) * incx;
    if (incy < 0) dy += (-n+1) * incy;
    for (i=0; i < n; i++) {
      *dy += da * (*dx);
      dx += incx;
      dy += incy;
    }
  }
  return;
}

/********************************************************************* 
 * Function sorts array1 and array2 into increasing order for array1 *
 *********************************************************************/
void svd_dsort2(long igap, long n, double *array1, double *array2) {
  double temp;
  long i, j, index;
  
  if (!igap) return;
  else {
    for (i = igap; i < n; i++) {
      j = i - igap;
      index = i;
      while (j >= 0 && array1[j] > array1[index]) {
        temp = array1[j];
        array1[j] = array1[index];
        array1[index] = temp;
        temp = array2[j];
        array2[j] = array2[index];
        array2[index] = temp;
        j -= igap;
        index = j + igap;
      }
    } 
  }
  svd_dsort2(igap/2,n,array1,array2);
}

/************************************************************** 
 * Function interchanges two vectors		     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
void svd_dswap(long n, double *dx, long incx, double *dy, long incy) {
  long i;
  double dtemp;
  
  if (n <= 0 || incx == 0 || incy == 0) return;
  if (incx == 1 && incy == 1) {
    for (i=0; i < n; i++) {
      dtemp = *dy;
      *dy++ = *dx;
      *dx++ = dtemp;
    }	
  }
  else {
    if (incx < 0) dx += (-n+1) * incx;
    if (incy < 0) dy += (-n+1) * incy;
    for (i=0; i < n; i++) {
      dtemp = *dy;
      *dy = *dx;
      *dx = dtemp;
      dx += incx;
      dy += incy;
    }
  }
}

/***************************************************************** 
 * Function finds the index of element having max. absolute value*
 * based on FORTRAN 77 routine from Linpack by J. Dongarra       *
 *****************************************************************/ 
long svd_idamax(long n, double *dx, long incx) {
  long ix,i,imax;
  double dtemp, dmax;
  
  if (n < 1) return(-1);
  if (n == 1) return(0);
  if (incx == 0) return(-1);
  
  if (incx < 0) ix = (-n+1) * incx;
  else ix = 0;
  imax = ix;
  dx += ix;
  dmax = fabs(*dx);
  for (i=1; i < n; i++) {
    ix += incx;
    dx += incx;
    dtemp = fabs(*dx);
    if (dtemp > dmax) {
      dmax = dtemp;
      imax = ix;
    }
  }
  return(imax);
}



/***********************************************************************
 *                                                                     *
 *				random()                               *
 *                        (double precision)                           *
 ***********************************************************************/
/***********************************************************************

   Description
   -----------

   This is a translation of a Fortran-77 uniform random number
   generator.  The code is based  on  theory and suggestions  given in
   D. E. Knuth (1969),  vol  2.  The argument to the function should 
   be initialized to an arbitrary integer prior to the first call to 
   random.  The calling program should  not  alter  the value of the
   argument between subsequent calls to random.  Random returns values
   within the interval (0,1).


   Arguments 
   ---------

   (input)
   iy	   an integer seed whose value must not be altered by the caller
	   between subsequent calls

   (output)
   random  a double precision random number between (0,1)

 ***********************************************************************/
double svd_random2(long *iy) {
   static long m2 = 0;
   static long ia, ic, mic;
   static double halfm, s;

   /* If first entry, compute (max int) / 2 */
   if (!m2) {
      m2 = 1 << (8 * (int)sizeof(int) - 2); 
      halfm = m2;

      /* compute multiplier and increment for linear congruential 
       * method */
      ia = 8 * (long)(halfm * atan(1.0) / 8.0) + 5;
      ic = 2 * (long)(halfm * (0.5 - sqrt(3.0)/6.0)) + 1;
      mic = (m2-ic) + m2;

      /* s is the scale factor for converting to floating point */
      s = 0.5 / halfm;
   }

   /* compute next random number */
   *iy = *iy * ia;

   /* for computers which do not allow integer overflow on addition */
   if (*iy > mic) *iy = (*iy - m2) - m2;

   *iy = *iy + ic;

   /* for computers whose word length for addition is greater than
    * for multiplication */
   if (*iy / 2 > m2) *iy = (*iy - m2) - m2;
  
   /* for computers whose integer overflow affects the sign bit */
   if (*iy < 0) *iy = (*iy + m2) + m2;

   return((double)(*iy) * s);
}

/************************************************************** 
 *							      *
 * Function finds sqrt(a^2 + b^2) without overflow or         *
 * destructive underflow.				      *
 *							      *
 **************************************************************/ 
/************************************************************** 

   Funtions used
   -------------

   UTILITY	dmax, dmin

 **************************************************************/ 
double svd_pythag(double a, double b) {
   double p, r, s, t, u, temp;

   ExcAssert(isfinite(a));
   ExcAssert(isfinite(b));

   p = svd_dmax(fabs(a), fabs(b));
   if (p != 0.0) {
      temp = svd_dmin(fabs(a), fabs(b)) / p;
      r = temp * temp; 
      t = 4.0 + r;

      int iter = 0;
      for (; iter != 1000 && t != 4.0;  ++iter) {
	 s = r / t;
	 u = 1.0 + 2.0 * s;
	 p *= u;
	 temp = s / u;
	 r *= temp * temp;
	 t = 4.0 + r;
      }

      if (iter == 1000) {
          throw MLDB::Exception("svd_pythag infinite loop: %f, %f",
                          a, b);
      }
   }
   return(p);
}

