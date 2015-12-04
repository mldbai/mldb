// sqlite3ppext.h
//
// The MIT License
//
// Copyright (c) 2012 Wongoo Lee (iwongu at gmail dot com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#ifndef SQLITE3PPEXT_H
#define SQLITE3PPEXT_H

#include "sqlite3pp.h"
#include <map>
#include <functional>
#include <memory>
#include <boost/type_traits.hpp>

namespace sqlite3pp
{
  namespace ext
  {

    class context : boost::noncopyable
    {
     public:
      explicit context(sqlite3_context* ctx, int nargs = 0, sqlite3_value** values = 0);

      int args_count() const;
      int args_bytes(int idx) const;
      int args_type(int idx) const;

      template <class T> T get(int idx) const {
        return get(idx, T());
      }

      void result(int value);
      void result(double value);
      void result(long long int value);
      void result(std::string const& value);
      void result(char const* value, bool fstatic = true);
      void result(void const* value, int n, bool fstatic = true);
      void result();
      void result(null_type);
      void result_copy(int idx);
      void result_error(char const* msg);

      void* aggregate_data(int size);
      int aggregate_count();

     private:
      int get(int idx, int) const;
      double get(int idx, double) const;
      long long int get(int idx, long long int) const;
      char const* get(int idx, char const*) const;
      std::string get(int idx, std::string) const;
      void const* get(int idx, void const*) const;

     private:
      sqlite3_context* ctx_;
      int nargs_;
      sqlite3_value** values_;
    };

    namespace
    {
      template <class R>
      void function0_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R ()>* f =
            static_cast<std::function<R ()>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)());
      }

      template <class R, class P1>
      void function1_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R (P1)>* f =
            static_cast<std::function<R (P1)>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)(c.context::get<P1>(0)));
      }

      template <class R, class P1, class P2>
      void function2_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R (P1, P2)>* f =
            static_cast<std::function<R (P1, P2)>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)(c.context::get<P1>(0), c.context::get<P2>(1)));
      }

      template <class R, class P1, class P2, class P3>
      void function3_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R (P1, P2, P3)>* f =
            static_cast<std::function<R (P1, P2, P3)>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2)));
      }

      template <class R, class P1, class P2, class P3, class P4>
      void function4_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R (P1, P2, P3, P4)>* f =
            static_cast<std::function<R (P1, P2, P3, P4)>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2), c.context::get<P4>(3)));
      }

      template <class R, class P1, class P2, class P3, class P4, class P5>
      void function5_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        std::function<R (P1, P2, P3, P4, P5)>* f =
            static_cast<std::function<R (P1, P2, P3, P4, P5)>*>(sqlite3_user_data(ctx));
        context c(ctx, nargs, values);
        c.result((*f)(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2), c.context::get<P4>(3), c.context::get<P5>(4)));
      }

    }


    class function : boost::noncopyable
    {
     public:
      typedef std::function<void (context&)> function_handler;
      typedef std::shared_ptr<void> pfunction_base;

      explicit function(database& db);

      int create(char const* name, function_handler h, int nargs = 0);

      template <class F> int create(char const* name, std::function<F> h) {
          fh_[name].reset(new std::function<F>(h));
        return create_function_impl<std::function<F>::arity, F>()(db_, fh_[name].get(), name);
      }

     private:
      template <int N, class F>
      struct create_function_impl;

      template <class F>
      struct create_function_impl<0, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
            typedef std::function<F> FT;
            typedef typename FT::result_type R;
            
            return sqlite3_create_function(db, name, 0, SQLITE_UTF8, fh,
                                           function0_impl<R>,
                                           0, 0);
        }
      };

      template <class F>
      struct create_function_impl<1, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
          typedef std::function<F> FT;
          typedef typename FT::result_type R;
          typedef typename FT::arg1_type P1;

          return sqlite3_create_function(db, name, 1, SQLITE_UTF8, fh,
                                         function1_impl<R, P1>,
                                         0, 0);
        }
      };

      template <class F>
      struct create_function_impl<2, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
          typedef std::function<F> FT;
          typedef typename FT::result_type R;
          typedef typename FT::arg1_type P1;
          typedef typename FT::arg2_type P2;

          return sqlite3_create_function(db, name, 2, SQLITE_UTF8, fh,
                                         function2_impl<R, P1, P2>,
                                         0, 0);
        }
      };

      template <class F>
      struct create_function_impl<3, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
          typedef std::function<F> FT;
          typedef typename FT::result_type R;
          typedef typename FT::arg1_type P1;
          typedef typename FT::arg2_type P2;
          typedef typename FT::arg3_type P3;

          return sqlite3_create_function(db, name, 3, SQLITE_UTF8, fh,
                                         function3_impl<R, P1, P2, P3>,
                                         0, 0);
        }
      };

      template <class F>
      struct create_function_impl<4, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
          typedef std::function<F> FT;
          typedef typename FT::result_type R;
          typedef typename FT::arg1_type P1;
          typedef typename FT::arg2_type P2;
          typedef typename FT::arg3_type P3;
          typedef typename FT::arg4_type P4;

          return sqlite3_create_function(db, name, 4, SQLITE_UTF8, fh,
                                         function4_impl<R, P1, P2, P3, P4>,
                                         0, 0);
        }
      };

      template <class F>
      struct create_function_impl<5, F> {
        int operator()(sqlite3* db, void* fh, char const* name) {
          typedef std::function<F> FT;
          typedef typename FT::result_type R;
          typedef typename FT::arg1_type P1;
          typedef typename FT::arg2_type P2;
          typedef typename FT::arg3_type P3;
          typedef typename FT::arg4_type P4;
          typedef typename FT::arg5_type P5;

          return sqlite3_create_function(db, name, 5, SQLITE_UTF8, fh,
                                         function5_impl<R, P1, P2, P3, P4, P5>,
                                         0, 0);
        }
      };

     private:
      sqlite3* db_;

      std::map<std::string, pfunction_base> fh_;
    };

    namespace
    {
      template <class T>
      void step0_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step();
      }

      template <class T, class P1>
      void step1_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step(c.context::get<P1>(0));
      }

      template <class T, class P1, class P2>
      void step2_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step(c.context::get<P1>(0), c.context::get<P2>(1));
      }

      template <class T, class P1, class P2, class P3>
      void step3_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2));
      }

      template <class T, class P1, class P2, class P3, class P4>
      void step4_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2), c.context::get<P4>(3));
      }

      template <class T, class P1, class P2, class P3, class P4, class P5>
      void step5_impl(sqlite3_context* ctx, int nargs, sqlite3_value** values)
      {
        context c(ctx, nargs, values);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        if (c.aggregate_count() == 1) new (t) T;
        t->step(c.context::get<P1>(0), c.context::get<P2>(1), c.context::get<P3>(2), c.context::get<P4>(3), c.context::get<P5>(4));
      }

      template <class T>
      void finishN_impl(sqlite3_context* ctx)
      {
        context c(ctx);
        T* t = static_cast<T*>(c.aggregate_data(sizeof(T)));
        c.result(t->finish());
        t->~T();
      }

    }

    class aggregate : boost::noncopyable
    {
     public:
      typedef std::function<void (context&)> function_handler;
      typedef std::shared_ptr<void> pfunction_base;

      explicit aggregate(database& db);

      int create(char const* name, function_handler s, function_handler f, int nargs = 1);

      template <class T>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 0, SQLITE_UTF8, 0, 0, step0_impl<T>, finishN_impl<T>);
      }

      template <class T, class P1>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 1, SQLITE_UTF8, 0, 0, step1_impl<T, P1>, finishN_impl<T>);
      }

      template <class T, class P1, class P2>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 2, SQLITE_UTF8, 0, 0, step2_impl<T, P1, P2>, finishN_impl<T>);
      }

      template <class T, class P1, class P2, class P3>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 3, SQLITE_UTF8, 0, 0, step3_impl<T, P1, P2, P3>, finishN_impl<T>);
      }

      template <class T, class P1, class P2, class P3, class P4>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 4, SQLITE_UTF8, 0, 0, step4_impl<T, P1, P2, P3, P4>, finishN_impl<T>);
      }

      template <class T, class P1, class P2, class P3, class P4, class P5>
      int create(char const* name) {
        return sqlite3_create_function(db_, name, 5, SQLITE_UTF8, 0, 0, step5_impl<T, P1, P2, P3, P4, P5>, finishN_impl<T>);
      }

     private:
      sqlite3* db_;

      std::map<std::string, std::pair<pfunction_base, pfunction_base> > ah_;
    };


  } // namespace ext

} // namespace sqlite3pp

#endif
