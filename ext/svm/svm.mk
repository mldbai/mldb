$(eval $(call library,svm,svm.cpp,))
$(eval $(call set_compile_option,svm.cpp,clang:-Wno-deprecated-declarations))