#
# azure-storage-cpp.mk
# Mich, 2017-01-13
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#
# Builds azure-storage-cpp lib. Files commented out are not built.
#
AZURE_STORAGE_CPP_SOURCE := $(shell cd mldb/ext/azure-storage-cpp && ls Microsoft.WindowsAzure.Storage/src/*.cpp)
#AZURE_STORAGE_CPP_SOURCE := #./Microsoft.WindowsAzure.Storage/samples/BlobsGettingStarted/Application.cpp \
./Microsoft.WindowsAzure.Storage/samples/BlobsGettingStarted/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/FilesGettingStarted/Application.cpp \
./Microsoft.WindowsAzure.Storage/samples/FilesGettingStarted/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/TablesGettingStarted/Application.cpp \
./Microsoft.WindowsAzure.Storage/samples/TablesGettingStarted/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/Channel9GoingNativeDemo2/NativeClientLibraryDemo2.cpp \
./Microsoft.WindowsAzure.Storage/samples/Channel9GoingNativeDemo2/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/Channel9GoingNativeDemo1/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/Channel9GoingNativeDemo1/NativeClientLibraryDemo1.cpp \
./Microsoft.WindowsAzure.Storage/samples/JsonPayloadFormat/Application.cpp \
./Microsoft.WindowsAzure.Storage/samples/JsonPayloadFormat/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/SamplesCommon/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/samples/QueuesGettingStarted/Application.cpp \
./Microsoft.WindowsAzure.Storage/samples/QueuesGettingStarted/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_blob_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/storage_exception_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_page_blob_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/retry_policy_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/file_test_base.cpp \
./Microsoft.WindowsAzure.Storage/tests/test_base.cpp \
./Microsoft.WindowsAzure.Storage/tests/blob_lease_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/unicode_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_append_blob_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/main.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_table_client_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/executor_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_file_share_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_file_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_queue_client_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/blob_streams_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_file_client_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_blob_client_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/read_from_secondary_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_storage_account_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/queue_test_base.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_table_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/table_test_base.cpp \
./Microsoft.WindowsAzure.Storage/tests/blob_test_base.cpp \
./Microsoft.WindowsAzure.Storage/tests/result_iterator_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/stdafx.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_blob_directory_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_blob_container_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/service_properties_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_block_blob_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_file_directory_test.cpp \
./Microsoft.WindowsAzure.Storage/tests/cloud_queue_test.cpp

AZURE_STORAGE_CPP_FLAGS := \
    -DBOOST_LOG_DYN_LINK \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes \
    -I/usr/include/libxml++-2.6 \
    -I/usr/include/glibmm-2.4 \
    -I/usr/lib/x86_64-linux-gnu/glibmm-2.4/include \
    -I/usr/include/glib-2.0 \
    -I/usr/lib/x86_64-linux-gnu/glib-2.0/include \
    -I/usr/lib/libxml++-2.6/include \
    -Wno-overloaded-virtual \
    -Wno-unused-value \
    -Wno-switch \
    -Wno-reorder \
    -Wno-return-type \


$(eval $(call set_compile_option,$(AZURE_STORAGE_CPP_SOURCE),$(AZURE_STORAGE_CPP_FLAGS)))
$(eval $(call library,azure_storage_cpp,$(AZURE_STORAGE_CPP_SOURCE),casablanca uuid xml++-2.6 boost_log pplx))
