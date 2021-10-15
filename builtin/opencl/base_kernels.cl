//#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

__kernel void
__blockFillArrayKernel(__global uint8_t * region,
                     uint64_t startOffsetInBytes,
                     uint64_t lengthInBytes,
                     __global uint8_t * blockData,
                     uint64_t blockLengthInBytes)
{
    //printf("Hello from kernel %ld\n", get_global_id(0));
    //if (get_global_id(0) == 0) {
    //    printf("\n\n\n\n\nfilling with %ld values of %ld bytes ", lengthInBytes / blockLengthInBytes, blockLengthInBytes);
    //    for (int i = 0;  i < blockLengthInBytes;  ++i) {
    //        printf("%02x ", blockData[i]);
    //    }
    //    printf("\n");
    //}

    // TODO: could be improved massively...
    for (uint64_t i = get_global_id(0);  i * blockLengthInBytes < lengthInBytes;  i += get_global_size(0)) {
        for (uint64_t j = 0;  j < blockLengthInBytes;  ++j) {
            region[i * blockLengthInBytes + startOffsetInBytes + j] = blockData[j];
        }
    }

    //if (get_global_id(0) == 0) {
    //    printf("\n\n\n\n\nfilling with %ld values of %ld bytes ", lengthInBytes / blockLengthInBytes, blockLengthInBytes);
    //    for (int i = 0;  i < blockLengthInBytes;  ++i) {
    //        printf("%02x ", blockData[i]);
    //    }
    //    printf("\n");
    //}
}

__kernel void
__zeroFillArrayKernel(__global uint8_t * region,
                      uint64_t startOffsetInBytes,
                      uint64_t lengthInBytes)
{
    size_t r = (size_t)region;

    if (r % 16 == 0 && startOffsetInBytes % 16 == 0 && lengthInBytes % 16 == 0) {
        __global ulong2 * qwordRegion = (__global ulong2 *)region;
        uint64_t startOffsetInQWords = startOffsetInBytes / 16;
        uint64_t lengthInQWords = lengthInBytes / 16;

        const ulong2 init = {0, 0};

        for (uint64_t i = get_global_id(0);  i < lengthInQWords;  i += get_global_size(0)) {
                qwordRegion[i + startOffsetInQWords] = init;
        }
        return;
    }

    if (r % 8 == 0 && startOffsetInBytes % 8 == 0 && lengthInBytes % 8 == 0) {
        __global uint64_t * dwordRegion = (__global uint64_t *)region;
        uint64_t startOffsetInDWords = startOffsetInBytes / 8;
        uint64_t lengthInDWords = lengthInBytes / 8;

        for (uint64_t i = get_global_id(0);  i < lengthInDWords;  i += get_global_size(0)) {
            dwordRegion[i + startOffsetInDWords] = 0;
        }
        return;
    }

    if (r % 4 == 0 && startOffsetInBytes % 4 == 0 && lengthInBytes % 4 == 0) {
        __global uint32_t * wordRegion = (__global uint32_t *)region;
        uint64_t startOffsetInWords = startOffsetInBytes / 4;
        uint64_t lengthInWords = lengthInBytes / 4;

        for (uint64_t i = get_global_id(0);  i < lengthInWords;  i += get_global_size(0)) {
            wordRegion[i + startOffsetInWords] = 0;
        }
        return;
    }
    // TODO: could be improved massively...
    for (uint64_t i = get_global_id(0);  i < lengthInBytes;  i += get_global_size(0)) {
        region[i + startOffsetInBytes] = 0;
    }
}
