

#       azure_storage_cpp

AZURE_BLOB_STORAGE_OPTIONS := \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes \
    -Wno-overloaded-virtual \
    -Wno-reorder \
    -Wno-unused-value \
    -Wno-unknown-pragmas

$(eval $(call set_compile_option,azure_blob_storage.cc,$(AZURE_BLOB_STORAGE_OPTIONS)))

