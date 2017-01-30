// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* training_data.cc
   Jeremy Barnes, 10 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.

   Training data class.
*/


#include "training_data.h"
#include "training_index.h"
#include "training_index_entry.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/sgi_numeric.h"
#include "mldb/base/exc_assert.h"
#include <boost/progress.hpp>
#include "mldb/jml/db/persistent.h"
#include "mldb/arch/demangle.h"


using namespace std;
using namespace ML::DB;



namespace ML {


/*****************************************************************************/
/* TRAINING_DATA                                                             */
/*****************************************************************************/


Training_Data::Training_Data()
{
}

Training_Data::
Training_Data(std::shared_ptr<const Feature_Space> feature_space)
    : dirty_(false)
{
    init(feature_space);
}

Training_Data::Training_Data(const Training_Data & other)
    : data_(other.data_), index_(other.index_),
      feature_space_(other.feature_space_), dirty_(other.dirty_)
{
}

Training_Data::~Training_Data()
{
}
    
void Training_Data::
init(std::shared_ptr<const Feature_Space> feature_space)
{
    feature_space_ = feature_space;
    clear();
}

// Can be used to spot-check that the indexing logic is correct.  Should be
// disabled normally as the checking is expensive.
bool check_filtered_indexes = false;

void
Training_Data::
initFiltered(const Training_Data & other,
             const std::vector<float> & keepExamples,
             const Feature & labelFeature,
             const std::vector<Feature> & keepFeatures)
{
    clear();

    ExcAssertEqual(other.example_count(), keepExamples.size());

    /// This vector contains the new example number for each of the
    /// examples in the original list, or -1 if it is filtered.
    /// The functions that implement the logic need to know this to
    /// efficiently initialize their internal data structures.
    vector<int> newExampleNumbers(keepExamples.size(), -1);

    int currentExample = 0;
    for (unsigned i = 0;  i < other.example_count();  ++i) {
        if (!keepExamples[i])
            continue;
        add_example(other.share(i));
        newExampleNumbers[i] = currentExample++;
    }

    Guard guard(index_lock);
    if (index_)
        throw Exception("preindex: already has index");
    index_.reset(new Dataset_Index());
    index_->initFiltered(other, newExampleNumbers, labelFeature, keepFeatures);
    dirty_ = false;

    if (check_filtered_indexes) {
        // Do it the old way (by re-indexing) and check we go the
        // same results back.
        guard.unlock();
    
        // Now check we got the same data
        auto filtered = index_;
        index_.reset();
        dirty_ = true;
        preindex(labelFeature, keepFeatures);
    
        auto toCheck = keepFeatures;
        toCheck.push_back(labelFeature);

        for (auto & f: toCheck) {
            auto & entry1 = index_->getFeatureIndex(f);
            auto & entry2 = filtered->getFeatureIndex(f);
    
            if (entry1.print_info() != entry2.print_info()) {
                cerr << "different info " << endl;
                cerr << entry1.print_info() << endl;
                cerr << entry2.print_info() << endl;
                throw MLDB::Exception("different info");
            }

            if (entry1.examples != entry2.examples) {
                cerr << "different examples" << endl;
                throw MLDB::Exception("different examples");
            }
        
            if (entry1.values != entry2.values) {
                cerr << "different values" << endl;
                throw MLDB::Exception("different values");
            }

            if (entry1.example_count != entry2.example_count) {
                cerr << "different example count" << endl;
                throw MLDB::Exception("different example count");
            }
        }
    }    
}

void Training_Data::clear()
{
    data_.clear();
    index_.reset();
    dirty_ = false;
}
    
void Training_Data::swap(Training_Data & other)
{
    std::swap(data_, other.data_);
    std::swap(index_, other.index_);
    std::swap(feature_space_, other.feature_space_);
    std::swap(dirty_, other.dirty_);
}
    
std::vector<Feature>
Training_Data::all_features() const
{
    return index().all_features();
}

void Training_Data::dump(const std::string & filename) const
{
    MLDB::filter_ostream stream(filename);
    dump(stream);
}

void Training_Data::dump(std::ostream & stream) const
{
    stream << feature_space_->print() << endl;
    for (unsigned i = 0;  i < data_.size();  ++i) {
        stream << feature_space_->print(*data_[i])
               << endl;
    }
}

void Training_Data::serialize(DB::Store_Writer & store) const
{
    store << string("TRAINING_DATA");  // tag
    store << compact_size_t(1);  // version
    store << compact_size_t(data_.size());

    for (unsigned i = 0;  i < data_.size();  ++i)
        feature_space()->serialize(store, *data_[i]);

    store << compact_size_t(12345);  // ending marker
}

void Training_Data::save(const std::string & filename) const
{
    Store_Writer store(filename);
    serialize(store);
}
    
void Training_Data::reconstitute(DB::Store_Reader & store)
{
    string id;
    store >> id;
    if (id != "TRAINING_DATA")
        throw Exception("Training_Data::reconsitute(): object in store had ID "
                        + id  + ", expected TRAINING_DATA");

    compact_size_t version(store);

    switch (version) {
    case 1: {
        clear();
        compact_size_t size(store);
        for (unsigned i = 0;  i < size;  ++i) {
            std::shared_ptr<Feature_Set> ex;
            feature_space()->reconstitute(store, ex);
            add_example(ex);
        }
        
        compact_size_t marker(store);
        if (marker != 12345)
            throw Exception("Training_Data::reconstitue(): end marker invalid "
                            "or not found");
        break;
    }
        
    default:
        throw Exception("Training_Data::reconstitute(): unknown version");
    }
}
    
void Training_Data::load(const std::string & filename)
{
    Store_Reader store(filename);
    reconstitute(store);
}

std::shared_ptr<Feature_Set> & Training_Data::modify(int example)
{
    std::shared_ptr<Feature_Set> & fs = data_.at(example);
    dirty_ = true;
    if (!fs.unique())
        fs.reset(fs->make_copy());
    return fs;
}

void Training_Data::
add(const Training_Data & other, bool merge_index)
{
    bool can_merge = !(other.dirty_ || dirty_);

    for (unsigned x = 0;  x < other.example_count();  ++x)
        add_example(other.share(x));

    if (can_merge && merge_index) {
        // TODO: really merge them
        // Don't need to do now; the dirty flags will take care of it!
#if 0
        index_->merge(other.index_);
#endif        
    }
}

int Training_Data::
add_example(const std::shared_ptr<Feature_Set> & example)
{
    example->sort();

    /* Add a new entry for the example. */
    int example_num = data_.size();
    data_.push_back(example);

    dirty_ = true;

    return example_num;
}

size_t Training_Data::
label_count(const Feature & predicted) const
{
    return feature_space()->info(predicted).value_count();
}

float
Training_Data::
modify_feature(int example_number,
               const Feature & feature,
               float new_val)
{
    std::shared_ptr<Feature_Set> & fs = data_[example_number];
    Mutable_Feature_Set * mut_fs = 0;
    
    float old_val = (*fs)[feature];

    if (old_val == new_val) return old_val;
            
    if (!mut_fs)
        mut_fs = dynamic_cast<Mutable_Feature_Set *>(fs.get());
    if (!mut_fs) {
        std::shared_ptr<Mutable_Feature_Set>
            mut(new Mutable_Feature_Set(fs->begin(), fs->end()));
        data_[example_number] = mut;
        mut_fs = mut.get();
    }

    notify_needs_reindex(feature);

    mut_fs->replace(feature, new_val);

    dirty_ = true;

    return old_val;
}

void
Training_Data::
fixup_grouping_features(const std::vector<Feature> & group_features,
                        std::vector<float> & offset)
{
    boost::timer t;

    size_t nf = group_features.size();

    bool ignore[nf];

    /* Categorical and string features can't be fixed up; we don't try to
       do it. */
    for (unsigned i = 0;  i < group_features.size();  ++i) {
        Feature_Info info = feature_space()->info(group_features[i]);
        if (info.value_count() != 0)
            ignore[i] = true;
        else ignore[i] = false;
    }
    
    //cerr << "fixup_grouping_features: features = " << group_features
    //     << " offsets = " << offset << endl;

    if (offset.empty())
        offset.resize(nf, 0.0);
    else if (offset.size() != group_features.size())
        throw Exception("fixup_grouping_features(): offsets don't match");

    vector<float> last(nf, 0.0);

    /* Get all features that have the grouping feature set. */
    for (unsigned x = 0;  x < data_.size();  ++x) {
        std::shared_ptr<Feature_Set> & fs = data_[x];

        //cerr << "example " << x << endl;

        for (unsigned f = 0;  f < nf;  ++f) {
            if (ignore[f]) continue;

            float group = (*fs)[group_features[f]];

            //cerr << "  group = " << group << " last = " << last[f] << endl;

            if (group < last[f])
                offset[f] += last[f] + 1.0;
            
            float new_group = group + offset[f];
            
            if (group != new_group)
                modify_feature(x, group_features[f], new_group);
            
            last[f] = group;
        }
    }
    
    for (unsigned f = 0;  f < nf;  ++f) {
        if (ignore[f]) continue;
        offset[f] += last[f] + 1.0;
    }

    //cerr << "fixup_grouping_features: " << t.elapsed() << "s" << endl;
}

void Training_Data::
preindex(const Feature & label, const std::vector<Feature> & features)
{
    Guard guard(index_lock);
    if (index_)
        throw Exception("preindex: already has index");

    //boost::timer timer;
    index_.reset(new Dataset_Index());
    index_->init(*this, label, features);
    dirty_ = false;
    //cerr << "preindex(): " << timer.elapsed() << "s for "
    //     << example_count() << " examples" << endl;
}

void Training_Data::preindex(const Feature & label)
{
    return;
    throw Exception("STUB", __PRETTY_FUNCTION__);
}

void Training_Data::preindex_features()
{
    return;
    throw Exception("STUB", __PRETTY_FUNCTION__);
}
    
vector<std::shared_ptr<Training_Data> >
Training_Data::
partition(const std::vector<float> & sizes_, bool random,
          const Feature & group_feature) const
{
    vector<std::shared_ptr<Training_Data> > output(sizes_.size());

    //cerr << "partitioning dataset" << endl;
    //dump(cerr);
    
    /* We do it differently depending upon whether or not we have a grouping
       feature. */
    if (group_feature == MISSING_FEATURE) {

        //cerr << "no group feature" << endl;

        /* This is a vector which tells us in which order we look at the
           examples. */
        vector<int> order(example_count());
        std::iota(order.begin(), order.end(), 0);
        if (random) std::random_shuffle(order.begin(), order.end());
        
        distribution<float> sizes(sizes_.begin(), sizes_.end());
        sizes.normalize();  sizes *= example_count();
        
        //cerr << "sizes = " << sizes << endl;
        
        unsigned ex = 0;
        for (unsigned i = 0;  i < sizes.size();  ++i) {
            /* Make an output training data object of the right type. */
            output[i].reset(make_type());
            output[i]->init(feature_space());
            
            /* Copy the data.  This is efficient since we use shared pointers to
               point to them; we are merely increasing the reference count of the
               pointer. */
            int n = (int)round(sizes[i]);
            
            /* Compensate for rounding errors. */
            while (ex + n > example_count()) --n;
            
            //cerr << "n = " << n << "  ex = " << ex << "  ex + n = " << ex + n
            //     << "  example_count = " << example_count() << endl;
            for (int x = ex;  x < ex + n;  ++x)
                output[i]->add_example(share(order[x]));
            
            ex += n;
        }
    }
    else {

        //cerr << "group feature" << endl;

        /* Get a list of groups. */
        Joint_Index group_dist
            = index().dist(group_feature, BY_EXAMPLE, IC_VALUE | IC_EXAMPLE);

        vector<float> groups;
        vector<vector<int> > group_examples;
        vector<int> group_numbers(example_count(), -1);

        float last_val = 0.0;
        for (Index_Iterator it = group_dist.begin();
             it != group_dist.end();  ++it) {
            float val = it->value();
            if (it == group_dist.begin() || val != last_val) {
                groups.push_back(val);
                group_examples.push_back(vector<int>());
                last_val = val;
            }
            group_examples.back().push_back(it->example());
            group_numbers[it->example()] = group_examples.size() - 1;
        }
        
        //cerr << "groups = " << groups << endl;

        int group_count = group_examples.size();

        //cerr << group_count << " groups" << endl;
        //cerr << example_count() << " examples" << endl;

        /* This is a vector which tells us in which order we look at the
           groups. */
        vector<int> order(group_count);
        std::iota(order.begin(), order.end(), 0);
        //cerr << "order = " << order << endl;
        if (random) std::random_shuffle(order.begin(), order.end());
        //cerr << "order = " << order << endl;
        
        distribution<float> sizes(sizes_.begin(), sizes_.end());
        sizes.normalize();  sizes *= group_count;
        
        //cerr << "sizes = " << sizes << endl;
        
        unsigned gr = 0;
        size_t total = 0;
        for (unsigned i = 0;  i < sizes.size();  ++i) {
            /* Make an output training data object of the right type. */
            output[i].reset(make_type());
            output[i]->init(feature_space());

            /* Copy the data.  This is efficient since we use shared pointers to
               point to them; we are merely increasing the reference count of the
               pointer. */
            int n = (int)round(sizes[i]);

            /* Compensate for rounding errors. */
            while (gr + n > group_count) --n;

            //cerr << "i = " << i << " gr = " << gr << " n = " << n << endl;
            
            //cerr << "adding " << n << " groups to dataset " << i << endl;
            for (int g = gr;  g < gr + n;  ++g) {
                //cerr << " " << groups[order[g]] << " ( ";
                for (int x = 0;  x < group_examples[order[g]].size();  ++x) {
                    output[i]->add_example(share(group_examples[order[g]][x]));
                    //cerr << group_examples[order[g]][x] << " ";
                }
                //cerr << " )  ";
            }
            cerr << endl;

            total += n;
            gr += n;
        }
    }
    
    return output;
}

Training_Data * Training_Data::make_copy() const
{
    return new Training_Data(*this);
}

Training_Data * Training_Data::make_type() const
{
    return new Training_Data();
}

const Dataset_Index & Training_Data::generate_index() const
{
    Guard guard(index_lock);
    if (!dirty_ && index_) return *index_;

    //boost::timer timer;
    index_.reset(new Dataset_Index());
    index_->init(*this);
    dirty_ = false;
    //cerr << "generate_index(): " << timer.elapsed() << "s for "
    //     << example_count() << " examples" << endl;
    return *index_;
}

size_t
Training_Data::
row_offset(size_t row) const
{
    throw Exception("Training_Data::row_offset(): class of type "
                    + demangle(typeid(*this).name())
                    + " doesn't support row offsets");
}

std::string
Training_Data::
row_comment(size_t row) const
{
    throw Exception("Training_Data::row_comment(): class of type "
                    + demangle(typeid(*this).name())
                    + " doesn't support row comments");
}

} // namespace ML

