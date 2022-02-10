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
#include "mldb/builtin/metal/compute_kernel_metal.h"

using namespace std;
using namespace MLDB;

using boost::unit_test::test_suite;

static struct RegisterKernels {

    RegisterKernels()
    {
        auto createReferenceCountingTestKernel = [] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernelTemplate>
        {
            auto result = std::make_shared<GridComputeKernelTemplate>(&context);
            result->kernelName = "referenceCountingTest";
            result->addParameter("result", "w", "u32[1]");
            result->addParameter("tmp", "w", "u32[1]");
            result->setGridExpression("[1]", "[32]");
            result->setComputeFunction("test_kernels", "simdGroupBarrierTestKernel");
            return result;
        };

        registerGridComputeKernel("referenceCountingTest", createReferenceCountingTestKernel);

        auto compileLibrary = [] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeFunctionLibrary>
        {
            std::string fileName = "mldb/builtin/metal/testing/test_kernels.metal";
            try {
                return MetalComputeFunctionLibrary::compileFromSourceFile(context, fileName);
            } catch (const MLDB::AnnotatedException & exc) {
                cerr << "Error: " << exc.details.asJson() << endl;
                throw;
            }
        };

        registerMetalLibrary("test_kernels", compileLibrary);

    }
} initKernels;

BOOST_AUTO_TEST_CASE( test_reference_counting )
{
    auto runtime = ComputeRuntime::getRuntimeForId(ComputeRuntimeId::METAL);
    auto context = runtime->getContext(array{ComputeDevice::defaultFor(ComputeRuntimeId::METAL)});
    auto queue = context->getQueue("test");
    auto kernel = context->getKernel("referenceCountingTest");
    auto result = context->allocUninitializedArraySync<uint32_t>("result", 32);
    auto boundKernel = kernel->bind(*queue, "result", result);
    queue->enqueue("run test", boundKernel, {});
    auto cpuResult = queue->transferToHostSync("get result", result);
    for (size_t i = 0;  i < 32;  ++i) {
        BOOST_CHECK_EQUAL(cpuResult.getConstSpan()[i], (32 * 31)/2);
    }
    auto result2 = result;
}
