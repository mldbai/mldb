/* json_stream.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/json_parsing.h"
#include "mldb/types/json_printing.h"
#include <optional>
#include "mldb/base/parse_context.h"
#include "mldb/base/exc_assert.h"
#include "mldb/ext/jsoncpp/value.h"


namespace MLDB {

struct JsonStreamParsingPosition {
    virtual ~JsonStreamParsingPosition() = default;
};

struct JsonStreamParsingContext {
    virtual ~JsonStreamParsingContext() = default;
    virtual std::optional<JsonParsingContext *> current() = 0;
    virtual bool next() = 0;
    virtual std::shared_ptr<const JsonStreamParsingPosition> savePosition() const = 0;
    virtual void restorePosition(std::shared_ptr<const JsonStreamParsingPosition> position) = 0;
};

struct ArrayJsonStreamParsingContext: public JsonStreamParsingContext {
    ArrayJsonStreamParsingContext(std::vector<Json::Value> values)
        : values_(std::move(values)), current_(values_.empty() ? DUMMY : values_[0])
    {
        //using namespace std;
        //cerr << "stream parsing " << values_.size() << " values" << endl;
    }

    static const Json::Value DUMMY;

    virtual ~ArrayJsonStreamParsingContext() = default;

    virtual std::optional<JsonParsingContext *> current() override
    {
        if (index_ >= values_.size())
            return std::nullopt;
        return &current_;
    }

    virtual bool next() override
    {
        if (index_ >= values_.size())
            return false;
        ++index_;
        current_.reset(values_[index_]);
        return true;
    }

    struct Position: public JsonStreamParsingPosition {
        const ArrayJsonStreamParsingContext * owner = nullptr;
        size_t index = 0;
        std::any position;
    };

    virtual std::shared_ptr<const JsonStreamParsingPosition> savePosition() const override
    {
        auto result = std::make_shared<Position>();
        result->owner = this;
        result->index = index_;
        result->position = ((StructuredJsonParsingContext &)current_).savePosition();
        return result;
    }

    virtual void restorePosition(std::shared_ptr<const JsonStreamParsingPosition> positionIn) override
    {
        const Position & position = dynamic_cast<const Position &>(*positionIn);
        ExcAssertEqual(position.owner, this);
        index_ = position.index;
        if (index_ < values_.size()) {
            current_.reset(values_[index_]);
        }
        else {
            current_.reset(DUMMY);
        }
        current_.restorePosition(position);
    }

private:
    std::vector<Json::Value> values_;
    size_t index_ = 0;
    StructuredJsonParsingContext current_;
};

struct JsonStreamPrintingContext {
    virtual ~JsonStreamPrintingContext() = default;
    virtual JsonPrintingContext & current() = 0;
    virtual void next() = 0;
    virtual void finish() = 0;
    virtual void take(JsonStreamParsingContext & context)
    {
        for (;;) {
            auto current = context.current();
            if (!current)
                break;
            Json::Value val = (*current)->expectJson();
            this->current().writeJson(std::move(val));
            next();
            context.next();
        }
    }
};

struct ArrayJsonStreamPrintingContext: public JsonStreamPrintingContext {
    ArrayJsonStreamPrintingContext()
        : context_(current_)
    {
    }

    virtual ~ArrayJsonStreamPrintingContext() = default;

    virtual JsonPrintingContext & current()
    {
        return context_;
    }

    virtual void next() override
    {
        values_.emplace_back(std::move(current_));
        current_ = Json::Value();
        context_.reset(current_);
    }

    virtual void finish() override
    {
        // no-op
    }
  
    std::vector<Json::Value> & values() { return values_; }
    const std::vector<Json::Value> & values() const { return values_; }

private:
    std::vector<Json::Value> values_;
    Json::Value current_;
    StructuredJsonPrintingContext context_;
};

struct JsonStreamProcessor {
    virtual ~JsonStreamProcessor() = default;
    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const = 0;
};

std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & program);
std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(const std::string & program);

} // namespace MLDB