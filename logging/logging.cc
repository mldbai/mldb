// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* logs.cc
   Eric Robert, 9 October 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Basic logs
*/

#include "mldb/logging/logging.h"
#include "mldb/base/exc_check.h"

#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <sys/time.h>


namespace MLDB {

void Logging::ConsoleWriter::head(char const * timestamp,
                                  char const * name,
                                  char const * function,
                                  char const * file,
                                  int line) {
    if(color) {
        stream << timestamp << " " << "\033[1;32m" << name << " ";
    }
    else {
        stream << timestamp << " " << name << " ";
    }
}

void Logging::ConsoleWriter::body(std::string const & content) {
    if(color) {
        stream << "\033[1;34m";
        stream.write(content.c_str(), content.size() - 1);
        stream << "\033[0m\n";
    }
    else {
        stream << content;
    }

    std::cerr << stream.str();
    stream.str("");
}

void Logging::FileWriter::head(char const * timestamp,
                               char const * name,
                               char const * function,
                               char const * file,
                               int line) {
    stream << timestamp << " " << name << " ";
}

void Logging::FileWriter::body(std::string const & content) {
    file << stream.str() << content;
    stream.str("");
}

void Logging::FileWriter::open(char const * filename, char const mode) {
    if(mode == 'a')
        file.open(filename, std::ofstream::app);
    else if (mode == 'w')
        file.open(filename);
    else
        throw MLDB::Exception("File mode not recognized");

    if(!file) {
        std::cerr << "unable to open log file '" << filename << "'" << std::endl;
    }
}

void Logging::JsonWriter::head(char const * timestamp,
                               char const * name,
                               char const * function,
                               char const * file,
                               int line) {
    stream << "{\"time\":\"" << timestamp
           << "\",\"name\":\"" << name
           << "\",\"call\":\"" << function
           << "\",\"file\":\"" << file
           << "\",\"line\":" << line
           << ",\"text\":\"";
}

void Logging::JsonWriter::body(std::string const & content) {
    stream.write(content.c_str(), content.size() - 1);
    stream << "\"}\n";
    if(!writer) {
        std::cerr << stream.str();
    }
    else {
        writer->body(stream.str());
    }

    stream.str("");
}

namespace {

struct Registry {
    std::mutex lock;
    std::unordered_map<std::string, std::shared_ptr<Logging::CategoryData> > categories;
};

Registry& getRegistry() {
    // Will leak but that's on program exit so who cares.
    static Registry* registry = new Registry;

    return *registry;
}

} // namespace anonymous

struct Logging::CategoryData {
    bool initialized;
    bool enabled;
    char const * name;
    std::shared_ptr<Writer> writer;
    std::stringstream stream;

    CategoryData * parent;
    std::vector<std::shared_ptr<CategoryData> > children;

    static std::shared_ptr<CategoryData> getRoot();
    static std::shared_ptr<CategoryData> get(char const * name);

    static std::shared_ptr<CategoryData> create(char const * name, char const * super, bool enabled);

    static void destroy(std::shared_ptr<CategoryData> & data);

    void activate(bool recurse = true);
    void deactivate(bool recurse = true);
    void writeTo(std::shared_ptr<Writer> output, bool recurse = true);

    ~CategoryData() {
    }

private:

    CategoryData(char const * name, bool enabled) :
        initialized(false),
        enabled(enabled),
        name(name),
        parent(nullptr) {
    }
};

std::shared_ptr<Logging::CategoryData> Logging::CategoryData::get(char const * name) {
    Registry& registry = getRegistry();

    auto it = registry.categories.find(name);
    return it != registry.categories.end() ? it->second : nullptr;
}

std::shared_ptr<Logging::CategoryData> Logging::CategoryData::getRoot() {
    std::shared_ptr<CategoryData> root = get("*");
    if (root) return root;

    root.reset(new CategoryData("*", true /* enabled */));
    getRegistry().categories["*"] = root;
    root->parent = root.get();
    root->writer = std::make_shared<ConsoleWriter>();

    return root;
}

std::shared_ptr<Logging::CategoryData> Logging::CategoryData::create(char const * name, char const * super, bool enabled) {
    Registry& registry = getRegistry();
    std::lock_guard<std::mutex> guard(registry.lock);

    std::shared_ptr<CategoryData> root = getRoot();
    std::shared_ptr<CategoryData> data = get(name);

    if (!data) {
        data.reset(new CategoryData(name, enabled));
        registry.categories[name] = data;
    }

    if (!data->initialized) {
        data->initialized = true;

        data->parent = get(super).get();
        if (!data->parent) {
            registry.categories[super].reset(data->parent = new CategoryData(super, enabled));
        }

        data->parent->children.push_back(data);

        if (data->parent->initialized) {
            data->writer = data->parent->writer;
        }
        else {
            data->writer = root->writer;
        }
    }

    return data;
}

void Logging::CategoryData::destroy(std::shared_ptr<CategoryData> & data) {
    if (data->parent == data.get()) return;

    // There are two uses:
    // 1.  The current reference
    // 2.  The registry

    Registry& registry = getRegistry();
    std::string name = data->name;

    std::lock_guard<std::mutex> guard(registry.lock);

    auto dataIt = registry.categories.find(name);
    if (dataIt == registry.categories.end())
        return;

    auto& children = data->parent->children;

    for (auto it = children.begin(), end = children.end();  it != end;
         ++it) {
        if (it->get() == data.get()) {
            children.erase(it);
            break;
        }
    }

    std::shared_ptr<CategoryData> root = getRoot();
    for (auto& child : data->children) {
        child->parent = root.get();
    }

    registry.categories.erase(dataIt);

    data.reset();
}

void Logging::CategoryData::activate(bool recurse) {
    enabled = true;
    if(recurse) {
        for(auto item : children) {
            item->activate(recurse);
        }
    }
}

void Logging::CategoryData::deactivate(bool recurse) {
    enabled = false;
    if(recurse) {
        for(auto item : children) {
            item->deactivate(recurse);
        }
    }
}

void Logging::CategoryData::writeTo(std::shared_ptr<Writer> output, bool recurse) {
    writer = output;
    if(recurse) {
        for(auto item : children) {
            item->writeTo(output, recurse);
        }
    }
}

Logging::Category& Logging::Category::root() {
    static Category root(CategoryData::getRoot());
    return root;
}

Logging::Category::Category(std::shared_ptr<CategoryData> data) :
    data(data) {
}

Logging::Category::Category(char const * name, Category & super, bool enabled) :
    data(CategoryData::create(name, super.name(), enabled)) {
}

Logging::Category::Category(char const * name, char const * super, bool enabled) :
    data(CategoryData::create(name, super, enabled)) {
}

Logging::Category::Category(char const * name, bool enabled) :
    data(CategoryData::create(name, "*", enabled)) {
}

Logging::Category::~Category()
{
    CategoryData::destroy(data);
}

char const * Logging::Category::name() const {
    return data->name;
}

bool Logging::Category::isEnabled() const {
    return data->enabled;
}

bool Logging::Category::isDisabled() const {
    return !data->enabled;
}

auto Logging::Category::getWriter() const -> std::shared_ptr<Writer> const &
{
    return data->writer;
}

void Logging::Category::activate(bool recurse) {
    data->activate(recurse);
}

void Logging::Category::deactivate(bool recurse) {
    data->deactivate(recurse);
}

void Logging::Category::writeTo(std::shared_ptr<Writer> output, bool recurse) {
    data->writeTo(output, recurse);
}

// This lock is a quick-fix for the case where a category is used by multiple
// threads. Note that this lock should either eventually be removed or replaced
// by a per category lock. Unfortunately the current setup makes it very
// difficult to pass the header information to the operator& so that everything
// can be dumped in the stream in one go.
namespace { std::mutex loggingMutex; }

std::ostream & Logging::Category::beginWrite(char const * fct, char const * file, int line) {
    loggingMutex.lock();

    timeval now;
    gettimeofday(&now, 0);
    char text[64];
    auto count = strftime(text, sizeof(text), "%Y-%m-%d %H:%M:%S", localtime(&now.tv_sec));
    int ms = now.tv_usec / 1000;
    sprintf(text + count, ".%03d", ms);
    data->writer->head(text, data->name, fct, file, line);
    return data->stream;
}

void Logging::Printer::operator&(std::ostream & stream) {
    std::stringstream & text = (std::stringstream &) stream;
    category.getWriter()->body(text.str());
    text.str("");

    loggingMutex.unlock();
}

void Logging::Thrower::operator&(std::ostream & stream) {

    std::stringstream & text = (std::stringstream &) stream;
    std::string message(text.str());
    text.str("");
    loggingMutex.unlock();

    throw MLDB::Exception(message);
}

} // namespace MLDB
