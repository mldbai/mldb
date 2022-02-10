#include <ukl>

SYNC_FUNCTION(v1, uint32_t, threadgroup_sum) (uint32_t val, uint16_t wg_lane, RWLOCAL(atomic_uint) tmp)
{
    auto simdgroup_lane = wg_lane % 32;
    auto simdgroup_num = wg_lane / 32;

    uint32_t wg_sum = SYNC_CALL(simdgroup_sum, val, simdgroup_lane, tmp + simdgroup_num);

    ukl_threadgroup_barrier();

    if (simdgroup_lane == 0 && simdgroup_num != 0) {
        atom_add(&tmp[0], wg_sum);
    }

    ukl_threadgroup_barrier();

    SYNC_RETURN(atom_load(&tmp[0]));
}

DEFINE_KERNEL2(test_kernels,
               simdGroupBarrierTestKernel,
               RWBUFFER, uint32_t,           result,              "[32]",
               RWLOCAL,  atomic_uint,        tmp,                 "[1]",
               GID0,     uint16_t,           id,,
               GSZ0,     uint16_t,           n,)
{
    uint32_t res = simdgroup_sum(id, id, tmp);

    result[id] = res;

    KERNEL_RETURN();
}

DEFINE_KERNEL2(test_kernels,
               threadGroupBarrierTestKernel,
               RWBUFFER, uint32_t,           result,              "[256]",
               RWLOCAL,  atomic_uint,        tmp,                 "[256/32]",
               GID0,     uint16_t,           id,,
               GSZ0,     uint16_t,           n,)
{
    uint32_t res = SYNC_CALL(threadgroup_sum, id, id, tmp);

    result[id] = res;

    KERNEL_RETURN();
}
