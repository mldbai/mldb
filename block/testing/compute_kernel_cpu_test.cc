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

enum CoroReturnKind {
    CRT_NONE,
    CRT_BARRIER,
    CRT_VALUE,
    CRT_EXCEPTION
};

DECLARE_ENUM_DESCRIPTION(CoroReturnKind);
DEFINE_ENUM_DESCRIPTION_INLINE(CoroReturnKind)
{
    addValue("NONE", CRT_NONE, "");
    addValue("BARRIER", CRT_BARRIER, "");
    addValue("VALUE", CRT_VALUE, "");
    addValue("EXCEPTION", CRT_EXCEPTION, "");
}

template<typename T>
struct coro_state_t {
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    // This will:
    // 1) Yield as many Barrier operations as there are synchronization points in the function
    // 2) Return the final value
    struct promise_type {
        CoroReturnKind kind_ = CRT_NONE;
        T value_;
        BarrierOp barrier_;
        std::exception_ptr exc_;

        coro_state_t get_return_object() { return {handle_type::from_promise(*this)}; }
        std::suspend_always initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() { kind_ = CRT_EXCEPTION; exc_ = std::current_exception(); }
        void return_value(T v) { kind_ = CRT_VALUE;  value_ = v; }
        std::suspend_always yield_value(BarrierOp barrier) { kind_ = CRT_BARRIER; barrier_ = barrier; return {}; }
    };

    handle_type h_;

    coro_state_t() {}
    coro_state_t(handle_type h)
        : h_(std::move(h))
    {

    }

    coro_state_t(coro_state_t && other)
         : h_(std::move(other.h_))
    {
        other.h_ = {};
    }


    coro_state_t & operator = (coro_state_t && other)
    {
        h_ = std::move(other.h_);  other.h_ = {};
        return *this;
    }

    ~coro_state_t()
    {
        //using namespace std;
        //cerr << "destroying coroutine handle" << endl;
        if (h_) h_.destroy();
    }

    bool has_barrier()
    {
        fill();
        return !h_.done() && h_.promise().kind_ == CRT_BARRIER;
    }

    bool has_result()
    {
        fill();
        return h_.done() && h_.promise().kind_ == CRT_VALUE;
    }

    BarrierOp get_barrier()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_BARRIER);
        return std::move(p.barrier_);
    }

    T get_result()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_VALUE);
        return std::move(p.value_);
    }

private:
    bool full_ = false;

    void fill()
    {
        if (!full_) {
            h_();
            if (h_.promise().exc_)
                std::rethrow_exception(h_.promise().exc_);
            full_ = true;
        }
    }
};

template<typename T>
inline T get_result(coro_state_t<T> & state)
{
    return state.get_result();
}

//inline void get_result(coro_state_t<void> & state)
//{
//    state.get_result();
//}

using kernel_coro = coro_state_t<int>;  // should be void, but that requires a specialization

#define simdgroup_barrier() co_yield BarrierOp{SIMD_GROUP_BARRIER, __FILE__, __LINE__}
#define threadgroup_barrier() co_yield BarrierOp{THREAD_GROUP_BARRIER, __FILE__, __LINE__}

#define SYNC_FUNCTION(Version, Name, Return) coro_state_t<Return> Name
#define SYNC_RETURN(Val) co_return Val;
#define SYNC_CALL(Fn, ...) \
    ({ auto coro = Fn(__VA_ARGS__);  while (coro.has_barrier()) { co_yield coro.get_barrier(); } get_result(coro); })




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
