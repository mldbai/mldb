
#ifndef BSONCXX_API_H
#define BSONCXX_API_H

#ifdef BSONCXX_STATIC
#  define BSONCXX_API
#  define BSONCXX_PRIVATE
#else
#  ifndef BSONCXX_API
#    ifdef BSONCXX_EXPORT
        /* We are building this library */
#      define BSONCXX_API __attribute__((visibility("default")))
#    else
        /* We are using this library */
#      define BSONCXX_API __attribute__((visibility("default")))
#    endif
#  endif

#  ifndef BSONCXX_PRIVATE
#    define BSONCXX_PRIVATE __attribute__((visibility("hidden")))
#  endif
#endif

#ifndef BSONCXX_DEPRECATED
#  define BSONCXX_DEPRECATED __attribute__ ((__deprecated__))
#endif

#ifndef BSONCXX_DEPRECATED_EXPORT
#  define BSONCXX_DEPRECATED_EXPORT BSONCXX_API BSONCXX_DEPRECATED
#endif

#ifndef BSONCXX_DEPRECATED_NO_EXPORT
#  define BSONCXX_DEPRECATED_NO_EXPORT BSONCXX_PRIVATE BSONCXX_DEPRECATED
#endif

#define DEFINE_NO_DEPRECATED 0
#if DEFINE_NO_DEPRECATED
# define BSONCXX_NO_DEPRECATED
#endif

#endif
