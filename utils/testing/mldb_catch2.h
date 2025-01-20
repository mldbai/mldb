#pragma once

#include "catch2/catch_all.hpp"
#include "mldb/arch/exception.h"

// Save the Catch2 CHECK_THROWS macro...
#define CATCH2_CHECK_THROWS(...) INTERNAL_CATCH_THROWS( "CHECK_THROWS", Catch::ResultDisposition::ContinueOnFailure, __VA_ARGS__ )
#define CATCH2_REQUIRE_THROWS(...) INTERNAL_CATCH_THROWS( "REQUIRE_THROWS", Catch::ResultDisposition::Normal, __VA_ARGS__ )

// ...and replace it with our own that disables exceptions
#undef CHECK_THROWS
#define CHECK_THROWS(...) do { MLDB_TRACE_EXCEPTIONS(false); CATCH2_CHECK_THROWS(__VA_ARGS__); } while (0)

#undef REQUIRE_THROWS
#define REQUIRE_THROWS(...) do { MLDB_TRACE_EXCEPTIONS(false); CATCH2_REQUIRE_THROWS(__VA_ARGS__); } while (0)
