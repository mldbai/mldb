/* xlsx_importer.cc
   Jeremy Barnes, 25 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Importer for Excel .xlsx files.  These files are actually .zip
   files full of .xml documents.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/ext/tinyxml2/tinyxml2.h"
#include "mldb/utils/log.h"


using namespace std;

namespace {
    const char * printDoc(tinyxml2::XMLDocument & doc) {
        tinyxml2::XMLPrinter printer;
        doc.Accept( &printer );
        return printer.CStr();
    }
}

namespace MLDB {


/*****************************************************************************/
/* XLSX IMPORTER                                                             */
/*****************************************************************************/

struct XlsxImporterConfig: public ProcedureConfig {
    static constexpr const char * name = "experimental.import.xlsx";

    Url dataFileUrl;
    PolyConfigT<Dataset> output;
};

DECLARE_STRUCTURE_DESCRIPTION(XlsxImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(XlsxImporterConfig);

XlsxImporterConfigDescription::
XlsxImporterConfigDescription()
{
    addParent<ProcedureConfig>();
    addField("dataFileUrl", &XlsxImporterConfig::dataFileUrl,
             "URL to load Excel workbook from");
    addField("output", &XlsxImporterConfig::output,
             "Configuration for output dataset");
}

struct SharedStrings {

    void load(std::streambuf * buf, shared_ptr<spdlog::logger> logger)
    {
        std::ostringstream allText;
        allText << buf;

        unique_ptr<tinyxml2::XMLDocument> xml(new tinyxml2::XMLDocument());
        xml->Parse(allText.str().c_str());

        TRACE_MSG(logger) << "loaded document:\n" << printDoc(*xml);

        using namespace tinyxml2;

        XMLHandle handle(*xml);

        XMLElement * sst = handle.FirstChildElement("sst").ToElement();

        if (!sst)
            throw HttpReturnException(400, "xlsx file SharedStrings have no sst element");


        unsigned numStrings;
        int res = sst->QueryUnsignedAttribute("uniqueCount", &numStrings);
        if (res != XML_NO_ERROR)
            throw HttpReturnException(400, "xlsx file SharedStrings have no uniqueCount");

        DEBUG_MSG(logger) << "got " << numStrings << " unique strings";

        strings.reserve(numStrings);



        for (XMLNode * node = sst->FirstChildElement();  node;
             node = node->NextSibling()) {
            XMLHandle handle(*node);

            XMLElement * text = handle.FirstChildElement("t").ToElement();

            if (!text)
                throw HttpReturnException(400, "xlsx file SharedStrings <si> element has no child <t> element");

            const char * t = text->GetText();

            Utf8String textStr(t ? t : "");

            DEBUG_MSG(logger) << "got string " << textStr;

            strings.emplace_back(std::move(textStr));
        }

        DEBUG_MSG(logger) << "read " << strings.size() << " unique strings";

        if (numStrings != strings.size()) {
            throw HttpReturnException(400, "xlsx file SharedStrings file consistency error: number of strings read doesn't match definition",
                                      "numDefined", numStrings,
                                      "numRead", strings.size());

        }

    }

    std::vector<Utf8String> strings;
};

const map<string, CellValue::CellType> FORMATS = {
    { "general", CellValue::FLOAT },
    { "0", CellValue::FLOAT },
    { "0.00", CellValue::FLOAT },
    { "#,##0", CellValue::FLOAT },
    { "#,##0.00", CellValue::FLOAT },
    { "0%", CellValue::FLOAT },
    { "0.00%", CellValue::FLOAT },
    { "0.00e+00", CellValue::FLOAT },
    { "mm-dd-yy", CellValue::TIMESTAMP },
    { "d-mmm-yy", CellValue::TIMESTAMP },
    { "d-mmm", CellValue::TIMESTAMP },
    { "mmm-yy", CellValue::TIMESTAMP },
    { "h:mm am/pm", CellValue::TIMEINTERVAL },
    { "h:mm:ss am/pm", CellValue::TIMEINTERVAL },
    { "h:mm", CellValue::TIMEINTERVAL },
    { "h:mm:ss", CellValue::TIMEINTERVAL },
    { "m/d/yy h:mm", CellValue::TIMESTAMP },
    { "#,##0 ;(#,##0)", CellValue::FLOAT },
    { "#,##0 ;[red](#,##0)", CellValue::FLOAT },
    { "#,##0.00;(#,##0.00)", CellValue::FLOAT },
    { "#,##0.00;[red](#,##0.00)", CellValue::FLOAT },
    { "mm:ss", CellValue::TIMEINTERVAL },
    { "[h]:mm:ss", CellValue::TIMEINTERVAL },
    { "mmss.0", CellValue::TIMEINTERVAL },
    { "##0.0e+0", CellValue::FLOAT },
    { "@", CellValue::FLOAT },
    { "yyyy\\-mm\\-dd", CellValue::TIMESTAMP },
    { "dd/mm/yy", CellValue::TIMESTAMP },
    { "hh:mm:ss", CellValue::TIMEINTERVAL },
    { "dd/mm/yy\\ hh:mm", CellValue::TIMESTAMP },
    { "dd/mm/yyyy hh:mm:ss", CellValue::TIMESTAMP },
    { "yy-mm-dd", CellValue::TIMESTAMP },
    { "d-mmm-yyyy", CellValue::TIMESTAMP },
    { "m/d/yy", CellValue::TIMESTAMP },
    { "m/d/yyyy", CellValue::TIMESTAMP },
    { "dd-mmm-yyyy", CellValue::TIMESTAMP },
    { "dd/mm/yyyy", CellValue::TIMESTAMP },
    { "mm/dd/yy h:mm am/pm", CellValue::TIMESTAMP },
    { "mm/dd/yy hh:mm", CellValue::TIMESTAMP },
    { "mm/dd/yyyy h:mm am/pm", CellValue::TIMESTAMP },
    { "mm/dd/yyyy hh:mm:ss", CellValue::TIMESTAMP },
    { "yyyy-mm-dd hh:mm:ss", CellValue::TIMESTAMP }
};

const map<int, string> STANDARD_FORMATS = {
    {     0, "general" },
    {     1, "0" },
    {     2, "0.00" },
    {     3, "#,##0" },
    {     4, "#,##0.00" },
    {     9, "0%" },
    {     10, "0.00%" },
    {     11, "0.00e+00" },
    {     12, "# ?/?" },
    {     13, "# \?\?/\?\?" },
    {     14, "mm-dd-yy" },
    {     15, "d-mmm-yy" },
    {     16, "d-mmm" },
    {     17, "mmm-yy" },
    {     18, "h:mm am/pm" },
    {     19, "h:mm:ss am/pm" },
    {     20, "h:mm" },
    {     21, "h:mm:ss" },
    {     22, "m/d/yy h:mm" },
    {     37, "#,##0 ;(#,##0)" },
    {     38, "#,##0 ;[red](#,##0)" },
    {     39, "#,##0.00;(#,##0.00)" },
    {     40, "#,##0.00;[red](#,##0.00)" },
    {     45, "mm:ss" },
    {     46, "[h]:mm:ss" },
    {     47, "mmss.0" },
    {     48, "##0.0e+0" },
    {     49, "@" }
};

struct Styles {

    struct Style {
        int numFormatId;
        std::string formatStr;
        CellValue::CellType repr;
    };

    std::vector<Style> styles;


    void load(std::streambuf * buf, shared_ptr<spdlog::logger> logger)
    {
        std::ostringstream allText;
        allText << buf;

        unique_ptr<tinyxml2::XMLDocument> xml(new tinyxml2::XMLDocument());
        xml->Parse(allText.str().c_str());

        //xml->Print();


        map<int, string> formats = STANDARD_FORMATS;

        using namespace tinyxml2;

        XMLHandle handle(*xml);

        XMLElement * numFmts
            = handle
            .FirstChildElement("styleSheet")
            .FirstChildElement("numFmts")
            .ToElement();

        if (numFmts) {
            for (XMLNode * node = numFmts->FirstChildElement("numFmt");  node;
                 node = node->NextSibling()) {

                XMLElement * element = node->ToElement();

                if (!element)
                    throw HttpReturnException(400, "xlsx worksheet number format entry is not an element");


                auto readAttr = [&] (const std::string & attr) -> std::string
                    {
                        const char * foundAttr = element->Attribute(attr.c_str());
                        if (!foundAttr)
                            return std::string();
                        return std::string(foundAttr);
                    };

                string formatCode = readAttr("formatCode");
                int numFormatId = std::stoi(readAttr("numFmtId"));

                formats[numFormatId] = formatCode;
            }
        }

        XMLElement * cellXfs
            = handle
            .FirstChildElement("styleSheet")
            .FirstChildElement("cellXfs")
            .ToElement();

        if (cellXfs) {
            for (XMLNode * node = cellXfs->FirstChildElement("xf");  node;
                 node = node->NextSibling()) {

                XMLElement * element = node->ToElement();
                if (!element)
                    throw HttpReturnException(400, "xlsx worksheet sheet entry is not an element");

                auto readAttr = [&] (const std::string & attr) -> std::string
                    {
                        const char * foundAttr = element->Attribute(attr.c_str());
                        if (!foundAttr)
                            return std::string();
                        return std::string(foundAttr);
                    };

                int numFormatId = std::stoi(readAttr("numFmtId"));

                std::string formatStr;
                auto it = formats.find(numFormatId);
                if (it != formats.end())
                    formatStr = it->second;

                CellValue::CellType type = CellValue::EMPTY;
                if (!formatStr.empty()) {
                    auto it = FORMATS.find(formatStr);
                    if (it != FORMATS.end())
                        type = it->second;
                    else if (formatStr.find("yy") != string::npos
                             || formatStr.find("YY") != string::npos
                             || formatStr.find("MMM") != string::npos)
                        type = CellValue::TIMESTAMP;
                    else if (formatStr.find("mm") != string::npos
                             || formatStr.find("MM") != string::npos
                             || formatStr.find("ss") != string::npos
                             || formatStr.find("SS") != string::npos)
                        type = CellValue::TIMEINTERVAL;
                }
                DEBUG_MSG(logger) << "style " << styles.size() << " with format "
                    << formatStr << " has num format " << numFormatId << " "
                    << formatStr << " " << type;

                styles.emplace_back(Style{ numFormatId, formatStr, type });
            }
        }
    }

    std::vector<Utf8String> strings;
};

struct Workbook {

    Workbook(shared_ptr<spdlog::logger> logger)
        : baseDate("1899-12-30"),
          logger(logger)
    {
    }

    Date timestamp;
    Date baseDate;
    shared_ptr<spdlog::logger> logger;

    struct Sheet {
        Utf8String name;
        Utf8String id;
        Utf8String relId;
        Utf8String filename;
    };

    std::vector<Sheet> sheets;

    void setSheetRelationship(Utf8String relId, Utf8String type, Utf8String target)
    {
        // only keep relationships of type /worksheet
        if (!type.endsWith("/worksheet"))
            return;

        for (auto & s: sheets) {
            if (s.relId == relId) {
                s.filename = target;
                return;
            }
        }

        // For now, we ignore relationships that we don't understand
    };

    void loadSheets(std::streambuf * buf, shared_ptr<spdlog::logger> logger)
    {
        std::ostringstream allText;
        allText << buf;

        unique_ptr<tinyxml2::XMLDocument> xml(new tinyxml2::XMLDocument());
        xml->Parse(allText.str().c_str());

        TRACE_MSG(logger) << "workbook XML contents\n" << printDoc(*xml);

        using namespace tinyxml2;

        XMLHandle handle(*xml);

        XMLElement * workbookPr
            = handle.FirstChildElement("workbook")
            .FirstChildElement("workbookPr")
            .ToElement();

        string ns;
        if (!workbookPr) {
            ns = "s:";
            workbookPr
                = handle.FirstChildElement((ns + "workbook").c_str())
                .FirstChildElement((ns + "workbookPr").c_str())
                .ToElement();
        }

        if (!workbookPr)
            throw HttpReturnException(400, "xlsx workbook not found");

        // date1904 attribute says that the epoch is 1904-01-01
        const char * foundAttr = workbookPr->Attribute("date1904");
        if (foundAttr && strcmp(foundAttr, "1") == 0) {
            baseDate = Date("1904-01-01");
        }

        XMLElement * sheets
            = handle.FirstChildElement((ns + "workbook").c_str())
            .FirstChildElement((ns + "sheets").c_str())
            .ToElement();

        if (!sheets)
            throw HttpReturnException(400, "xlsx workbook has no sheets element");

        for (XMLNode * node = sheets->FirstChildElement((ns + "sheet").c_str());
             node;  node = node->NextSibling()) {
            XMLElement * element = node->ToElement();
            if (!element)
                throw HttpReturnException(400, "xlsx workbook sheet entry is not an element");

            auto readAttr = [&] (const std::string & attr) -> Utf8String
                {
                    const char * foundAttr = element->Attribute(attr.c_str());
                    if (!foundAttr)
                        return Utf8String();
                    return Utf8String(foundAttr);
                };

            Utf8String name = readAttr("name");
            Utf8String sheetId = readAttr("sheetId");
            Utf8String relId = readAttr("r:id");

            DEBUG_MSG(logger) << "sheet " << name << " " << sheetId << " " << relId;

            Sheet sheet{std::move(name), std::move(sheetId), std::move(relId)};

            this->sheets.emplace_back(std::move(sheet));

        }

        DEBUG_MSG(logger) << "got " << this->sheets.size() << " sheets in workbook";
    }

    // Map the worksheets in the excel notebook to filenames in the
    // zip file, by extracting the relationships table.
    void loadRelationships(std::streambuf * buf,
                           shared_ptr<spdlog::logger> logger)
    {
        std::ostringstream allText;
        allText << buf;

        unique_ptr<tinyxml2::XMLDocument> xml(new tinyxml2::XMLDocument());
        xml->Parse(allText.str().c_str());

        DEBUG_MSG(logger) << "workbook relationship XML contents";

        //xml->Print();

        using namespace tinyxml2;

        XMLHandle handle(*xml);

        auto el = handle.FirstChildElement().ToElement();
        if (!el)
            throw HttpReturnException(400, "xlsx worksheet has no relationships element");
        string name = el->Value();
        string ns;
        auto pos = name.find("Relationships");
        if (pos != string::npos)
            ns = string(name, 0, pos);

        XMLElement * rels
            = handle.FirstChildElement((ns + "Relationships").c_str())
            .ToElement();

        if (!rels)
            throw HttpReturnException(400, "xlsx worksheet has no relationships element");

        for (XMLNode * node = rels->FirstChildElement((ns + "Relationship").c_str());
             node;
             node = node->NextSibling()) {
            XMLElement * element = node->ToElement();
            if (!element)
                throw HttpReturnException(400, "xlsx worksheet Relationship entry is not an element");

            auto readAttr = [&] (const std::string & attr) -> Utf8String
                {
                    const char * foundAttr = element->Attribute(attr.c_str());
                    if (!foundAttr)
                        return Utf8String();
                    return Utf8String(foundAttr);
                };

            Utf8String name = readAttr("Id");
            Utf8String type = readAttr("Type");
            Utf8String target = readAttr("Target");

            setSheetRelationship(name, type, target);
        }
    }
};

struct Sheet {

    Sheet(std::streambuf * buf,
          const Workbook & workbook,
          const SharedStrings & strings,
          const Styles & styles,
          shared_ptr<spdlog::logger> logger)
    {
        std::ostringstream allText;
        allText << buf;

        unique_ptr<tinyxml2::XMLDocument> xml(new tinyxml2::XMLDocument());
        xml->Parse(allText.str().c_str());

        TRACE_MSG(logger) << "workbook sheet contents" << printDoc(*xml);

        using namespace tinyxml2;

        XMLHandle handle(*xml);

        XMLElement * data
            = handle.FirstChildElement("worksheet")
            .FirstChildElement("sheetData")
            .ToElement();

        if (!data)
            throw HttpReturnException(400, "xlsx worksheet has no sheetData element");

        for (XMLNode * node = data->FirstChildElement("row");  node;
             node = node->NextSibling()) {
            XMLElement * element = node->ToElement();
            if (!element)
                throw HttpReturnException(400, "xlsx worksheet Relationship entry is not an element");

            auto readAttr = [&] (const std::string & attr) -> Utf8String
                {
                    const char * foundAttr
                        = element->Attribute(attr.c_str());
                    if (!foundAttr)
                        return Utf8String();
                    return Utf8String(foundAttr);
                };

            // What is the row index?
            int64_t rowIndex = CellValue::parse(readAttr("r")).toInt();

            Row row;
            row.index = rowIndex;

            int64_t colIndex = 0;

            for (XMLNode * colNode = element->FirstChildElement("c");  colNode;
                 colNode = colNode->NextSibling()) {

                XMLElement * colElement = colNode->ToElement();

                if (!colElement)
                    throw HttpReturnException(400, "xlsx sheet column is not an element");

                auto readAttr = [&] (const std::string & attr) -> Utf8String
                    {
                        const char * foundAttr
                            = colElement->Attribute(attr.c_str());
                        if (!foundAttr)
                            return Utf8String();
                        return Utf8String(foundAttr);
                    };

                Utf8String type = readAttr("t");
                std::string cellid = readAttr("r").rawString();  // r stands for "reference"
                Utf8String style = readAttr("s");

                CellValue value;
                Utf8String contents;

                XMLNode * v = colNode->FirstChildElement("v");
                XMLElement * ve = v ? v->ToElement(): nullptr;

                if (ve) {
                    if (ve->GetText())
                        contents = ve->GetText();

                    TRACE_MSG(logger) << "type = " << type;
                    TRACE_MSG(logger) << "style = " << style;

                    if (type == "s") {
                        // shared string
                        int index = std::stoi(contents.rawString());
                        value = CellValue(strings.strings.at(index));
                    }
                    else if (!style.empty()) {
                        int styleNum = CellValue::parse(style).toInt();
                        const Styles::Style & style = styles.styles.at(styleNum);

                        switch (style.repr) {
                        case CellValue::TIMESTAMP: {
                            double offset = CellValue::parse(contents).toDouble();
                            value = workbook.baseDate.plusDays(offset);
                            break;
                        }
                        case CellValue::TIMEINTERVAL: {
                            uint16_t months = 0;
                            uint16_t days = 0;
                            uint16_t seconds = CellValue::parse(contents).toDouble();
                            value = CellValue::fromMonthDaySecond(months, days, seconds);
                            break;
                        }
                        case CellValue::FLOAT:  // fall through
                        case CellValue::EMPTY: // fall through
                            // These shouldn't occur
                        case CellValue::INTEGER:
                        case CellValue::ASCII_STRING:
                        case CellValue::UTF8_STRING:
                        default:
                            value = CellValue::parse(contents);
                        }
                    }
                    else if (type == "b" /* boolean */ || type.empty()) {
                        // generic...
                        value = CellValue::parse(contents);
                    }
                    else {
                        // Probably a date... we should handle those
                        INFO_MSG(logger) << "cell has unknown type";
                        INFO_MSG(logger) << "type = " << type << " cellid = " << cellid
                             << " s = " << style << " c " << contents;
                    }
                }

                // Get the column out of it
                std::string columnPart;

                if (cellid.empty()) {
                    ++colIndex;
                }
                else {
                    colIndex = 0;
                    size_t numLetters = 0;
                    while (numLetters < cellid.length() && isalpha(cellid[numLetters]))
                        ++numLetters;
                    if (numLetters == 1) {
                        colIndex = toupper(cellid[0]) - 'A';
                    }
                    else if (numLetters == 2) {
                        colIndex
                            = (toupper(cellid[0]) - 'A' + 1) * 26
                            + toupper(cellid[1]) - 'A';
                    }
                    else if (numLetters == 3) {
                        colIndex
                            = (toupper(cellid[0]) - 'A' + 1) * 26 * 26
                            + (toupper(cellid[1]) - 'A' + 1) * 26
                            + toupper(cellid[2]) - 'A';
                    }
                    else if (numLetters == 4) {
                        colIndex
                            = (toupper(cellid[0]) - 'A' + 1) * 26 * 26 * 26
                            + (toupper(cellid[0]) - 'A' + 1) * 26 * 26
                            + (toupper(cellid[1]) - 'A' + 1) * 26
                            + toupper(cellid[2]) - 'A';
                    }
                    else throw HttpReturnException(400, "Unable to parse Cell ID '" + cellid + "'");
                }

                DEBUG_MSG(logger) << "cell " << cellid << " has value " << jsonEncodeStr(value);
                DEBUG_MSG(logger) << "row " << rowIndex << " column " << colIndex;
                row.columns.emplace_back(colIndex, std::move(value));
            }

            rows.emplace_back(std::move(row));
        }
    }

    struct Row {
        int64_t index;   ///< Row index, 1-based
        std::vector<std::tuple<int64_t, CellValue> > columns;
    };

    vector<Row> rows;

}; // struct Sheet

struct XlsxImporter: public Procedure {

    XlsxImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<XlsxImporterConfig>();
    }

    XlsxImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        SharedStrings strings;
        Workbook workbook(logger);
        Styles styles;
        std::string savedRelationships;
        bool sheetsLoaded = false;
        auto runProcConf = applyRunConfOverProcConf(config, run);

        workbook.timestamp = Date::positiveInfinity();

        auto onFile = [&] (const std::string & prefix,
                           const FsObjectInfo & info,
                           const OpenUriObject & open,
                           int depth)
            {
                workbook.timestamp.setMin(info.lastModified);

                DEBUG_MSG(logger) << "got file " << prefix;

                auto fragPos = prefix.rfind('#');
                if (fragPos == string::npos)
                    throw HttpReturnException(500, "Couldn't find in filename");

                string internalFilename(prefix, fragPos + 1);

                DEBUG_MSG(logger) << "got internal filename " << internalFilename;

                if (internalFilename == "xl/sharedStrings.xml") {
                    // 1.  Load up the shared strings, which are required to
                    //     interpret the values in the cells
                    //
                    //     Note that some Excel files have no shared strings table.

                    strings.load(open({}).buf, logger);
                }
                else if (internalFilename == "xl/workbook.xml") {
                    // 2.  Load up the workbook, which tells us what our sheets
                    //     are.

                    workbook.loadSheets(open({}).buf, logger);
                    sheetsLoaded = true;
                    if (!savedRelationships.empty()) {
                        std::istringstream stream(std::move(savedRelationships));
                        workbook.loadRelationships(stream.rdbuf(), logger);
                    }

                }
                else if (internalFilename == "xl/_rels/workbook.xml.rels") {
                    if (sheetsLoaded)
                        workbook.loadRelationships(open({}).buf, logger);
                    else {
                        std::ostringstream stream;
                        stream << open({}).buf;
                        savedRelationships = stream.str();
                    }
                }
                else if (internalFilename == "xl/styles.xml") {
                    styles.load(open({}).buf, logger);
                }

                return true;
            };

        forEachUriObject(
            "archive+" + runProcConf.dataFileUrl.toDecodedString(),
            onFile);

        // Create the output dataset

        std::shared_ptr<Dataset> output;

        if (!runProcConf.output.type.empty()
            || !runProcConf.output.id.empty()) {
            output = obtainDataset(server, runProcConf.output);
        }

        // 4.  Load the worksheets, one by one
        for (auto & sheetEntry: workbook.sheets) {
            Utf8String filename =
                "archive+" + runProcConf.dataFileUrl.toDecodedString()
                + "#xl/" + sheetEntry.filename;
            filter_istream sheetStream(filename.rawString());

            Sheet sheet(sheetStream.rdbuf(), workbook, strings, styles, logger);

            DEBUG_MSG(logger) << "sheet had " << sheet.rows.size() << " rows";

            auto getColName = [] (int64_t colIndex)
                {
                    string result;

                    if (colIndex < 26) {
                        result = char('A' + (colIndex));
                    }
                    else {
                        result = char('A' + (colIndex % 26)) + result;
                        colIndex /= 26;
                        while (colIndex) {
                            result = char('A' + (colIndex % 26) - 1) + result;
                            colIndex /= 26;
                        }
                    }

                    return ColumnPath(result);
                };

            if (output && !sheet.rows.empty()) {
                int maxRowIndex = sheet.rows.back().index;
                int indexLength = MLDB::format("%d", maxRowIndex).length();

                for (auto & row: sheet.rows) {
                    MatrixNamedRow outputRow;
                    outputRow.rowHash = outputRow.rowName
                        = RowPath(sheetEntry.name + MLDB::format(":%0*d", indexLength, row.index));

                    for (auto & col: row.columns) {
                        outputRow.columns.emplace_back(getColName(std::get<0>(col)),
                                                       std::get<1>(col),
                                                       workbook.timestamp);
                    }

                    output->recordRow(outputRow.rowName, outputRow.columns);
                }
            }
        }
        output->commit();
        return RunOutput();
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    XlsxImporterConfig procConfig;
};

static RegisterProcedureType<XlsxImporter, XlsxImporterConfig>
regXlsx(builtinPackage(),
        "Import an Excel workbook into MLDB",
        "procedures/XlsxImporter.md.html",
        nullptr /* static route */,
        { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB

