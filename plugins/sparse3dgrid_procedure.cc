
// This file is part of MLDB. Copyright 2016 MLDB.ai. All rights reserved.

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/value_function.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/types/optional.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/vector_description.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/types/optional_description.h"
#include "jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/analytics.h"
#include "mldb/server/dataset_context.h"
#include "mldb/ext/eigen/Eigen/Core"

#include "mldb/types/sparse3dgrid.h"

namespace MLDB {

template<typename T>
using Sparse3DGridi = Sparse3DGrid<T, int, Eigen::Vector3i>;

/*****************************************************************************/
/*****************************************************************************/

struct SparseGridConfig: public ProcedureConfig {
    InputQuery trainingData;
};

DECLARE_STRUCTURE_DESCRIPTION(SparseGridConfig);

DEFINE_STRUCTURE_DESCRIPTION(SparseGridConfig);

SparseGridConfigDescription::
SparseGridConfigDescription()
{
    addParent<ProcedureConfig>();

    addField("trainingData", &SparseGridConfig::trainingData,
             "Points to partition.");
}

struct SparseGridInput {    
    ExpressionValue pos;
    size_t N;
};

DECLARE_STRUCTURE_DESCRIPTION(SparseGridInput);

DEFINE_STRUCTURE_DESCRIPTION(SparseGridInput);

SparseGridInputDescription::
SparseGridInputDescription()
{
     addFieldDesc("pos", &SparseGridInput::pos,
                 "",
                 makeExpressionValueDescription
                     (new EmbeddingValueInfo({ 3}, ST_INT32)));
     addField("N", &SparseGridInput::N,
                  "");
}

struct SparseGridOutput {
    ExpressionValue embedding;
};

DECLARE_STRUCTURE_DESCRIPTION(SparseGridOutput);

DEFINE_STRUCTURE_DESCRIPTION(SparseGridOutput)

SparseGridOutputDescription::
SparseGridOutputDescription()
{
    addFieldDesc("v", &SparseGridOutput::embedding,
                 "",
                 makeExpressionValueDescription
                     (new EmbeddingValueInfo({ -1, -1, -1}, ST_FLOAT32)));
}


struct SparseGridFunction: public ValueFunctionT<SparseGridInput, SparseGridOutput> {
    SparseGridFunction(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress)
        : BaseT(owner), myGrid(10, 5, 10)
    {
        this->functionConfig = config.params.convert<SparseGridConfig>();

        SqlExpressionMldbScope context(server);
        ExcAssert(functionConfig.trainingData.stm);
        const auto& stm = *functionConfig.trainingData.stm;
        ExcAssert(stm.from);
        BoundTableExpression table = stm.from->bind(context);

        std::vector<std::shared_ptr<SqlExpression> > calc;
        const PathElement xElement("x");
        const PathElement yElement("y");
        const PathElement zElement("z");
        const PathElement valueElement("value");

        size_t count = 0;

        auto processor = [&] (NamedRowValue & row_,
                                   const std::vector<ExpressionValue> & calc)
        {
            const auto& values = row_.columns;

            int x = 0.0f;
            int y = 0.0f;
            int z = 0.0f;
            float value = 0.0f;
            bool found[4] = {false, false, false, false};

            for (const auto& v : values) {

                if (std::get<0>(v) == xElement) {
                    x = std::get<1>(v).toInt();
                    found[0] = true;
                }
                else if (std::get<0>(v) == yElement) {
                    y = std::get<1>(v).toInt();
                    found[1] = true;
                }
                else if (std::get<0>(v) == zElement) {
                    z = std::get<1>(v).toInt();
                    found[2] = true;
                }
                else if (std::get<0>(v) == valueElement) {
                    value = std::get<1>(v).toDouble();
                    found[3] = true;
                }
            }
            ++count;
            myGrid.insert(Eigen::Vector3i(x,y,z), value);
            return true;
        };    

        iterateDataset(stm.select, *(table.dataset), table.asName, stm.when, *(stm.where),
                        calc, {processor, false}, stm.orderBy, stm.offset, stm.limit,
                        nullptr);
    }

    SparseGridConfig functionConfig;
    Sparse3DGridi<float> myGrid;

    virtual Any getStatus() const
    {
        return Any();
    }

    virtual SparseGridOutput call(SparseGridInput input) const
    {
        using namespace std;

        SqlExpressionMldbScope scope(server);
        SqlRowScope rowScope;

        auto pos = input.pos.getEmbedding();

        Eigen::Vector3i queryPos(pos[0], pos[1], pos[2]);
        Eigen::Vector3i offset(input.N, input.N, input.N);
        Eigen::Vector3i min = queryPos - offset;

        auto values = myGrid.queryBoundingBox(queryPos - offset, queryPos + offset);
        size_t dimsize = 1 + 2*input.N;

        std::shared_ptr<float> cube(new float[dimsize*dimsize*dimsize],
                                      [] (float * p) { delete[] p; });

        for (const auto& p : values) {
            auto rel = p.first - min;
            int offset = (rel.x() * dimsize * dimsize) + (rel.y() * dimsize) + rel.z();
            cube.get()[offset] = p.second;
        }

        auto tensor = ExpressionValue::embedding
            (Date::notADate(), cube, ST_FLOAT32, DimsVector{ dimsize, dimsize, dimsize, 1 });

        SparseGridOutput output;
        output.embedding = tensor;

        return output;
    }
};

namespace {

RegisterFunctionType<SparseGridFunction, SparseGridConfig>
regClassifyFunction(builtinPackage(),
                    "sparsegrid.getEmbedding",
                    "Description",
                    "procedures/ExperimentProcedure.md.html");

} // file scope

}