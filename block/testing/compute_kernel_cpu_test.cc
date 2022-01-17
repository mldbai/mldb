/* compute_kernel_cpu_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/block/compute_kernel_cpu.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;
using namespace MLDB;

using boost::unit_test::test_suite;

#include "mldb/block/compute_kernel_cpu_shims.h"

SYNC_FUNCTION(v1, simdgroup_sum2, uint32_t) (uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    simdgroup_barrier();

    atom_add(tmp + 0, val);

    simdgroup_barrier();

    SYNC_RETURN(tmp[0]);
}

SYNC_FUNCTION(v1, threadgroup_sum2, uint32_t) (uint32_t val, uint16_t wg_lane, __local atomic_uint * tmp)
{
    auto simdgroup_lane = wg_lane % 32;
    auto simdgroup_num = wg_lane / 32;

    uint32_t wg_sum = SYNC_CALL(simdgroup_sum2, val, simdgroup_lane, tmp + simdgroup_num);

    threadgroup_barrier();

    if (simdgroup_lane == 0 && simdgroup_num != 0) {
        atom_add(tmp, wg_sum);
    }

    threadgroup_barrier();

    SYNC_RETURN(tmp[0]);
}

DEFINE_KERNEL2(test_kernels,
               simdGroupBarrierTestKernel,
               BUFFER,  uint32_t,           result,              "[32]",
               LOCAL,   atomic_uint,        tmp,                 "[1]",
               GID0,    uint16_t,           id,,
               GSZ0,    uint16_t,           n,)
{
    uint32_t res = SYNC_CALL(simdgroup_sum2, id, id, tmp);

    result[id] = res;

    KERNEL_RETURN();
}

DEFINE_KERNEL2(test_kernels,
               threadGroupBarrierTestKernel,
               BUFFER,  uint32_t,           result,              "[256]",
               LOCAL,   atomic_uint,        tmp,                 "[256/32]",
               GID0,    uint16_t,           id,,
               GSZ0,    uint16_t,           n,)
{
    uint32_t res = SYNC_CALL(threadgroup_sum2, id, id, tmp);

    result[id] = res;

    KERNEL_RETURN();
}

static struct RegisterKernels {

    RegisterKernels()
    {
        auto createSimdGroupBarrierTestKernel = [] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernelTemplate>
        {
            auto result = std::make_shared<GridComputeKernelTemplate>(&context);
            result->kernelName = "simdGroupBarrierTest";
            result->addParameter("result", "w", "u32[1]");
            result->addParameter("tmp", "w", "u32[1]");
            result->setGridExpression("[1]", "[32]");
            result->setComputeFunction("test_kernels", "simdGroupBarrierTestKernel");
            return result;
        };

        registerGridComputeKernel("simdGroupBarrierTest", createSimdGroupBarrierTestKernel);

        auto createThreadGroupBarrierTestKernel = [] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernelTemplate>
        {
            auto result = std::make_shared<GridComputeKernelTemplate>(&context);
            result->kernelName = "threadGroupBarrierTest";
            result->addParameter("result", "w", "u32[1]");
            result->addParameter("tmp", "w", "u32[256/32]");
            result->setGridExpression("[1]", "[256]");
            result->setComputeFunction("test_kernels", "threadGroupBarrierTestKernel");
            return result;
        };

        registerGridComputeKernel("threadGroupBarrierTest", createThreadGroupBarrierTestKernel);
    }
} initKernels;

BOOST_AUTO_TEST_CASE( test_simdgroup_barrier )
{
    auto runtime = ComputeRuntime::getRuntimeForId(ComputeRuntimeId::CPU);
    auto context = runtime->getContext(array{ComputeDevice::host()});
    auto queue = context->getQueue("test");
    auto kernel = context->getKernel("simdGroupBarrierTest");
    auto result = context->allocUninitializedArraySync<uint32_t>("result", 32);
    auto boundKernel = kernel->bind(*queue, "result", result);
    queue->enqueue("run test", boundKernel, {});
    auto cpuResult = queue->transferToHostSync("get result", result);
    for (size_t i = 0;  i < 32;  ++i) {
        BOOST_CHECK_EQUAL(cpuResult.getConstSpan()[i], (32 * 31)/2);
    }
}

BOOST_AUTO_TEST_CASE( test_threadgroup_barrier )
{
    auto runtime = ComputeRuntime::getRuntimeForId(ComputeRuntimeId::CPU);
    auto context = runtime->getContext(array{ComputeDevice::host()});
    auto queue = context->getQueue("test");
    auto kernel = context->getKernel("threadGroupBarrierTest");
    auto result = context->allocUninitializedArraySync<uint32_t>("result", 256);
    auto boundKernel = kernel->bind(*queue, "result", result);
    queue->enqueue("run test", boundKernel, {});
    auto cpuResult = queue->transferToHostSync("get result", result);
    for (size_t i = 0;  i < 256;  ++i) {
        BOOST_CHECK_EQUAL(cpuResult.getConstSpan()[0], (256 * 255)/2);
    }
}
