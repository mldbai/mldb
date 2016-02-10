// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** text_line_dataset.cc
    Jeremy Barnes, 11 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Dataset that exposes text, line by line.
*/

#include "text_line_dataset.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/lightweight_hash.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/compact_vector_value_description.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"


using namespace std;


namespace Datacratic {
namespace MLDB {

namespace {

const ColumnName lineNumber("lineNumber");
const ColumnName lineText("lineText");
const ColumnHash lineNumberHash(lineNumber);
const ColumnHash lineTextHash(lineText);

} // file scope

/*****************************************************************************/
/* TEXT LINE INTERNAL REPRESENTATION                                         */
/*****************************************************************************/

struct TextLineDataset::Itl: public ColumnIndex, public MatrixView {

    Itl(const std::string & filename)
    {
        // For now... later we can memory map and only keep the offsets,
        // or stream through on a query

        ML::filter_istream stream(filename);

        while (stream) {
            string line;
            std::getline(stream, line);
            if (!stream && stream.gcount() == 0)
                break;
            lines.emplace_back(std::move(line));
            RowName rowName(to_string(lines.size()));
            rowIndex[rowName] = lines.size();
        }
    }
    
    vector<string> lines;
    ML::Lightweight_Hash<RowHash, int64_t> rowIndex;
    Date ts;

     struct TextLineRowStream : public RowStream {

        TextLineRowStream()
        {}

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<TextLineRowStream>();
            return ptr;
        }

        virtual void initAt(size_t start){
            lineNum = start;
        }

        virtual RowName next() {
            return RowName(to_string((lineNum++) + 1));            
        }

       int64_t lineNum;

    };

    virtual const ColumnStats &
    getColumnStats(const ColumnName & ch, ColumnStats & stats) const
    {
        throw HttpReturnException(400, "Column operations not available on text datasets");
    }

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        throw HttpReturnException(400, "Column operations not available on text datasets");
    }

    virtual uint64_t getColumnRowCount(const ColumnName & column) const
    {
        throw HttpReturnException(400, "Column operations not available on text datasets");
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return column == lineNumber || column == lineText;
    }

    virtual std::vector<ColumnName> getColumnNames() const
    {
        return { lineNumber, lineText };
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        vector<RowName> result;
        for (int64_t i = 0;  i < lines.size();  ++i) {
            result.push_back(RowName(to_string(i + 1)));
        }
        
        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        vector<RowHash> result;
        for (int64_t i = 0;  i < lines.size();  ++i) {
            result.push_back(RowHash(RowName(to_string(i + 1))));
        }
        
        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        return rowIndex.count(rowName);
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        return rowIndex.count(rowHash);
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        MatrixNamedRow result;
        result.rowName = rowName;
        result.rowHash = rowName;
        
        auto it = rowIndex.find(result.rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in text dataset");
        }
        int64_t line = it->second;
        if (!rowName.stringEqual(std::to_string(line)))
            throw HttpReturnException(400, "Row not found in text dataset");

        result.columns.emplace_back(lineNumber, line, ts);
        result.columns.emplace_back(lineText, lines.at(line - 1), ts);

        return result;
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in text dataset");
        }
        int64_t line = it->second;

        return RowName(std::to_string(line));
    }

    virtual MatrixEvent getEvent(const RowHash & rowHash, Date ts) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in text dataset");
        }

        int64_t line = it->second;

        MatrixEvent result;
        result.rowName = RowName(std::to_string(line));
        result.rowHash = rowHash;
        result.timestamp = ts;
        if (ts != this->ts)
            return result;


        result.columns.emplace_back(lineNumber, line);
        result.columns.emplace_back(lineText, lines.at(line - 1));

        return result;
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        if (column == lineNumberHash)
            return lineNumber;
        else if (column == lineTextHash)
            return lineText;
        throw HttpReturnException(400, "Line text dataset has no column named " + column.toString());
    }

    virtual size_t getRowCount() const
    {
        return lines.size();
    }

    virtual size_t getColumnCount() const
    {
        return 2;
    }
};


/*****************************************************************************/
/* TEXT LINE DATASET                                                         */
/*****************************************************************************/

TextLineDataset::
TextLineDataset(MldbServer * owner,
                       PolyConfig config,
                       const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    ExcAssert(!config.id.empty());
    
    auto params = config.params.convert<PersistentDatasetConfig>();
    
    itl.reset(new Itl(params.dataFileUrl.toString()));
}

TextLineDataset::
TextLineDataset(MldbServer * owner)
    : Dataset(owner)
{
}

TextLineDataset::
~TextLineDataset()
{
}

Any
TextLineDataset::
getStatus() const
{
    Json::Value result;
    return result;
}

std::pair<Date, Date>
TextLineDataset::
getTimestampRange() const
{
    return { itl->ts, itl->ts };
}

std::shared_ptr<MatrixView>
TextLineDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
TextLineDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
TextLineDataset::
getRowStream() const
{
    return make_shared<TextLineDataset::Itl::TextLineRowStream>();
}

namespace {

RegisterDatasetType<TextLineDataset, PersistentDatasetConfig>
regTextLine(builtinPackage(),
       "text.line",
       "Exposes a text file as a dataset, with one line per row",
       "datasets/TextLineDataset.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic

