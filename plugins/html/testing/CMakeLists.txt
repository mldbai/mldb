# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Adding a test for css_selector_test with the specified libraries and the 'boost' option
add_mldb_test(css_selector_test "mldb_html_plugin;mldb_engine;arch;types;value_description" "boost")

# Adding an MLDB unit test for MLDB-1162-html-plugin.js with html
add_mldb_unit_test(MLDB-1162-html-plugin.js html)
