# S2 geometry library support for MLDB

# command to generate this list:
# find ext/s2geometry/src -name "*.cc" | sort | grep -v '_test\.cc' | grep -v testing | sed 's!ext/s2geometry/src/s2/!\t!' | sed 's!$! \\!'

S2_CC_FILES:= \
	encoded_s2cell_id_vector.cc \
	encoded_s2point_vector.cc \
	encoded_s2shape_index.cc \
	encoded_string_vector.cc \
	id_set_lexicon.cc \
	mutable_s2shape_index.cc \
	r2rect.cc \
	s1angle.cc \
	s1chord_angle.cc \
	s1interval.cc \
	s2boolean_operation.cc \
	s2buffer_operation.cc \
	s2builder.cc \
	s2builder_graph.cc \
	s2builderutil_closed_set_normalizer.cc \
	s2builderutil_find_polygon_degeneracies.cc \
	s2builderutil_get_snapped_winding_delta.cc \
	s2builderutil_lax_polygon_layer.cc \
	s2builderutil_lax_polyline_layer.cc \
	s2builderutil_s2point_vector_layer.cc \
	s2builderutil_s2polygon_layer.cc \
	s2builderutil_s2polyline_layer.cc \
	s2builderutil_s2polyline_vector_layer.cc \
	s2builderutil_snap_functions.cc \
	s2cap.cc \
	s2cell.cc \
	s2cell_id.cc \
	s2cell_index.cc \
	s2cell_union.cc \
	s2centroids.cc \
	s2closest_cell_query.cc \
	s2closest_edge_query.cc \
	s2closest_point_query.cc \
	s2contains_vertex_query.cc \
	s2convex_hull_query.cc \
	s2coords.cc \
	s2crossing_edge_query.cc \
	s2debug.cc \
	s2earth.cc \
	s2edge_clipping.cc \
	s2edge_crosser.cc \
	s2edge_crossings.cc \
	s2edge_distances.cc \
	s2edge_tessellator.cc \
	s2error.cc \
	s2furthest_edge_query.cc \
	s2hausdorff_distance_query.cc \
	s2latlng.cc \
	s2latlng_rect.cc \
	s2latlng_rect_bounder.cc \
	s2lax_loop_shape.cc \
	s2lax_polygon_shape.cc \
	s2lax_polyline_shape.cc \
	s2loop.cc \
	s2loop_measures.cc \
	s2max_distance_targets.cc \
	s2measures.cc \
	s2memory_tracker.cc \
	s2metrics.cc \
	s2min_distance_targets.cc \
	s2padded_cell.cc \
	s2point_compression.cc \
	s2point_region.cc \
	s2pointutil.cc \
	s2polygon.cc \
	s2polyline.cc \
	s2polyline_alignment.cc \
	s2polyline_measures.cc \
	s2polyline_simplifier.cc \
	s2predicates.cc \
	s2projections.cc \
	s2r2rect.cc \
	s2region.cc \
	s2region_coverer.cc \
	s2region_intersection.cc \
	s2region_term_indexer.cc \
	s2region_union.cc \
	s2shape_index.cc \
	s2shape_index_buffered_region.cc \
	s2shape_index_measures.cc \
	s2shape_measures.cc \
	s2shape_nesting_query.cc \
	s2shapeutil_build_polygon_boundaries.cc \
	s2shapeutil_coding.cc \
	s2shapeutil_contains_brute_force.cc \
	s2shapeutil_conversion.cc \
	s2shapeutil_edge_iterator.cc \
	s2shapeutil_get_reference_point.cc \
	s2shapeutil_visit_crossing_edge_pairs.cc \
	s2text_format.cc \
	s2wedge_relations.cc \
	s2winding_operation.cc \
	util/bits/bit-interleave.cc \
	util/coding/coder.cc \
	util/coding/varint.cc \
	util/math/exactfloat/exactfloat.cc \
	util/math/mathutil.cc \
	util/units/length-units.cc \


ifeq ($(toolchain),gcc)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs
endif
ifeq ($(toolchain),gcc5)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs
endif
ifeq ($(toolchain),gcc6)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs -Wno-attributes -Wno-comment -Wno-bool-compare -Wno-return-type
endif
ifeq ($(toolchain),gcc7)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs -Wno-attributes -Wno-comment -Wno-bool-compare -Wno-return-type
endif
ifeq ($(toolchain),gcc8)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs -Wno-attributes -Wno-comment -Wno-bool-compare -Wno-return-type -Wno-class-memaccess
endif
ifeq ($(toolchain),gcc9)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs -Wno-attributes -Wno-comment -Wno-bool-compare -Wno-return-type -Wno-class-memaccess
endif
ifeq ($(toolchain),gcc10)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs -Wno-attributes -Wno-comment -Wno-bool-compare -Wno-return-type -Wno-class-memaccess
endif
ifeq ($(toolchain),gcc14)
S2_WARNING_OPTIONS:=-Wno-comment -Wno-maybe-uninitialized
endif
ifeq ($(toolchain),clang)
S2_WARNING_OPTIONS:=-Wno-parentheses -Wno-absolute-value -Wno-unused-local-typedef -Wno-unused-const-variable -Wno-format -Wno-dynamic-class-memaccess -Wno-unused-private-field -Wno-range-loop-analysis -Wno-ambiguous-reversed-operator -Wno-unused-but-set-variable
endif

S2_COMPILE_OPTIONS:=-Imldb/ext/s2geometry/src -Imldb/ext/abseil-cpp -DS2_USE_EXACTFLOAT $(OPENSSL_INCLUDE_FLAGS)

$(eval $(call set_compile_option,$(S2_CC_FILES),$(S2_COMPILE_OPTIONS) $(S2_WARNING_OPTIONS)))

$(eval $(call library,s2,$(S2_CC_FILES),crypto abseil))
