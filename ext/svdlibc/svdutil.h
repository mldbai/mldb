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

#ifndef SVDUTIL_H
#define SVDUTIL_H

#include "svdlib.h"

#define SAFE_FREE(a) {if (a) {free(a); a = NULL;}}

/* Allocates an array of longs. */
extern long *svd_longArray(long size, char empty, const char *name);
/* Allocates an array of doubles. */
extern double *svd_doubleArray(long size, char empty, const char *name);

extern void svd_debug(const char *fmt, ...);
extern void svd_error(const char *fmt, ...);
extern void svd_fatalError(const char *fmt, ...);


/************************************************************** 
 * returns |a| if b is positive; else fsign returns -|a|      *
 **************************************************************/ 
inline double svd_fsign(double a, double b) {
  if ((a>=0.0 && b>=0.0) || (a<0.0 && b<0.0))return(a);
  else return -a;
}

/************************************************************** 
 * returns the larger of two double precision numbers         *
 **************************************************************/ 
inline double svd_dmax(double a, double b) {
   return (a > b) ? a : b;
}

/************************************************************** 
 * returns the smaller of two double precision numbers        *
 **************************************************************/ 
inline double svd_dmin(double a, double b) {
  return (a < b) ? a : b;
}

/************************************************************** 
 * returns the larger of two integers                         *
 **************************************************************/ 
inline long svd_imax(long a, long b) {
  return (a > b) ? a : b;
}

/************************************************************** 
 * returns the smaller of two integers                        *
 **************************************************************/ 
inline long svd_imin(long a, long b) {
  return (a < b) ? a : b;
}

/************************************************************** 
 * Function scales a vector by a constant.     		      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern void svd_dscal(long n, double da, double *dx, long incx);

/************************************************************** 
 * function scales a vector by a constant.	     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern void svd_datx(long n, double da, double *dx, long incx, double *dy, long incy);

/************************************************************** 
 * Function copies a vector x to a vector y	     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern void svd_dcopy(long n, double *dx, long incx, double *dy, long incy);

/************************************************************** 
 * Function forms the dot product of two vectors.      	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern double svd_ddot(long n, double *dx, long incx, double *dy, long incy);

/************************************************************** 
 * Constant times a vector plus a vector     		      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern void svd_daxpy (long n, double da, double *dx, long incx, double *dy, long incy);

/********************************************************************* 
 * Function sorts array1 and array2 into increasing order for array1 *
 *********************************************************************/
extern void svd_dsort2(long igap, long n, double *array1, double *array2);

/************************************************************** 
 * Function interchanges two vectors		     	      *
 * Based on Fortran-77 routine from Linpack by J. Dongarra    *
 **************************************************************/ 
extern void svd_dswap(long n, double *dx, long incx, double *dy, long incy);

/***************************************************************** 
 * Function finds the index of element having max. absolute value*
 * based on FORTRAN 77 routine from Linpack by J. Dongarra       *
 *****************************************************************/ 
extern long svd_idamax(long n, double *dx, long incx);

/***********************************************************************
 *                                                                     *
 *				random2()                              *
 *                        (double precision)                           *
 ***********************************************************************/
extern double svd_random2(long *iy);

/************************************************************** 
 *							      *
 * Function finds sqrt(a^2 + b^2) without overflow or         *
 * destructive underflow.				      *
 *							      *
 **************************************************************/ 
extern double svd_pythag(double a, double b);


#endif /* SVDUTIL_H */
