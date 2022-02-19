
CATCH2_SOURCE:= \
	src/catch2/benchmark/internal/catch_benchmark_combined_tu.cpp \
	src/catch2/benchmark/detail/catch_stats.cpp \
	src/catch2/catch_session.cpp \
	src/catch2/catch_test_case_info.cpp \
	src/catch2/catch_assertion_result.cpp \
	src/catch2/catch_totals.cpp \
	src/catch2/catch_test_spec.cpp \
	src/catch2/internal/catch_section.cpp \
	src/catch2/internal/catch_floating_point_helpers.cpp \
	src/catch2/internal/catch_stringref.cpp \
	src/catch2/internal/catch_main.cpp \
	src/catch2/internal/catch_clara.cpp \
	src/catch2/internal/catch_exception_translator_registry.cpp \
	src/catch2/internal/catch_stream.cpp \
	src/catch2/internal/catch_assertion_handler.cpp \
	src/catch2/internal/catch_enforce.cpp \
	src/catch2/internal/catch_test_case_registry_impl.cpp \
	src/catch2/internal/catch_tag_alias_registry.cpp \
	src/catch2/internal/catch_singletons.cpp \
	src/catch2/internal/catch_fatal_condition_handler.cpp \
	src/catch2/internal/catch_wildcard_pattern.cpp \
	src/catch2/internal/catch_list.cpp \
	src/catch2/internal/catch_run_context.cpp \
	src/catch2/internal/catch_combined_tu.cpp \
	src/catch2/internal/catch_test_case_tracker.cpp \
	src/catch2/internal/catch_random_number_generator.cpp \
	src/catch2/internal/catch_result_type.cpp \
	src/catch2/internal/catch_debugger.cpp \
	src/catch2/internal/catch_source_line_info.cpp \
	src/catch2/internal/catch_string_manip.cpp \
	src/catch2/internal/catch_xmlwriter.cpp \
	src/catch2/internal/catch_console_colour.cpp \
	src/catch2/internal/catch_case_insensitive_comparisons.cpp \
	src/catch2/internal/catch_test_registry.cpp \
	src/catch2/internal/catch_enum_values_registry.cpp \
	src/catch2/internal/catch_test_spec_parser.cpp \
	src/catch2/internal/catch_random_seed_generation.cpp \
	src/catch2/internal/catch_debug_console.cpp \
	src/catch2/internal/catch_reporter_registry.cpp \
	src/catch2/internal/catch_textflow.cpp \
	src/catch2/internal/catch_context.cpp \
	src/catch2/internal/catch_commandline.cpp \
	src/catch2/internal/catch_output_redirect.cpp \
	src/catch2/catch_registry_hub.cpp \
	src/catch2/catch_message.cpp \
	src/catch2/catch_timer.cpp \
	src/catch2/catch_approx.cpp \
	src/catch2/generators/internal/catch_generators_combined_tu.cpp \
	src/catch2/matchers/catch_matchers_templated.cpp \
	src/catch2/matchers/catch_matchers_floating_point.cpp \
	src/catch2/matchers/internal/catch_matchers_combined_tu.cpp \
	src/catch2/matchers/catch_matchers_string.cpp \
	src/catch2/catch_tostring.cpp \
	src/catch2/reporters/catch_reporter_sonarqube.cpp \
	src/catch2/reporters/catch_reporter_junit.cpp \
	src/catch2/reporters/catch_reporter_xml.cpp \
	src/catch2/reporters/catch_reporter_streaming_base.cpp \
	src/catch2/reporters/catch_reporter_console.cpp \
	src/catch2/reporters/catch_reporter_tap.cpp \
	src/catch2/reporters/catch_reporter_combined_tu.cpp \
	src/catch2/reporters/catch_reporter_automake.cpp \
	src/catch2/reporters/catch_reporter_teamcity.cpp \
	src/catch2/reporters/catch_reporter_cumulative_base.cpp \
	src/catch2/reporters/catch_reporter_compact.cpp \
	src/catch2/catch_version.cpp \
	src/catch2/interfaces/catch_interfaces_reporter.cpp \
	src/catch2/interfaces/catch_interfaces_combined_tu.cpp \
	src/catch2/catch_config.cpp \
	src/catch2/reporters/catch_reporter_listening.cpp \

CATCH2_INCLUDE_PATHS:=mldb/ext/catch2/src
CATCH2_GCC_FLAGS:=-Wno-maybe-uninitialized -Wno-array-bounds -Wno-format-overflow -Wno-stringop-truncation -Wno-stringop-overflow
CATCH2_CLANG_FLAGS:=-Wno-maybe-uninitialized -Wno-format-overflow -Wno-stringop-truncation -Wno-stringop-overflow -Wno-unknown-warning-option

CATCH2_FLAGS= \
	$(if $(findstring gcc,$(toolchain)),$(CATCH2_GCC_FLAGS)) \
	$(if $(findstring clang,$(toolchain)),$(CATCH2_CLANG_FLAGS)) \
	$(foreach path,$(CATCH2_INCLUDE_PATHS), -I $(path))


# NOTE: to find this, run cmake in the ext/catch2 directory, and then
# cat config.h | grep '#define' | sed 's/#define /-D/' | sed 's/ /=/' | tr '\n' ' '
# cp config.h ../catch2-config-$(OSNAME)-$(ARCH).h
CATCH2_DEFINES_Linux_x86_64:=
CATCH2_LIBS_Linux_x86_64:=

CATCH2_DEFINES_Linux_aarch64:=
CATCH2_LIBS_Linux_aarch64:=

CATCH2_DEFINES_Darwin_x86_64:=
CATCH2_LIBS_Darwin_x86_64:=

CATCH2_DEFINES_Darwin_arm64:=
CATCH2_LIBS_Darwin_arm64:=

CATCH2_DEFINES:=$(CATCH2_DEFINES_$(OSNAME)_$(ARCH))
CATCH2_LIBS:=$(CATCH2_LIBS_$(OSNAME)_$(ARCH))
#$(if $(CATCH2_DEFINES),,$(error CATCH2_DEFINES_$(OSNAME)_$(ARCH) not defined (unknown arch $(ARCH)).  Please define in catch2.mk))

$(eval $(call set_compile_option,$(CATCH2_SOURCE),-Imldb/ext/catch2/catch2 $(CATCH2_DEFINES) $(CATCH2_FLAGS)))

CATCH2_LIB_NAME:=catch2

$(eval $(call library,$(CATCH2_LIB_NAME),$(CATCH2_SOURCE),$(CATCH2_LIBS)))

CATCH2_INCLUDE_DIR:=$(PWD)/catch2
DEPENDS_ON_CATCH2_INCLUDES:=$(LIB)/$(CATCH2_LIB_NAME)$(SO_EXTENSION)
DEPENDS_ON_CATCH2_LIB:=$(LIB)/$(CATCH2_LIB_NAME)$(SO_EXTENSION)

catch2:	$(DEPENDS_ON_CATCH2_LIB) $(DEPENDS_ON_CATCH2_INCLUDE)
