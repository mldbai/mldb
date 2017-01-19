/* boosting_core_parallel.h                                        -*- C++ -*-
   Jeremy Barnes, 19 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   The core of boosting, parallelized.
*/

#pragma once

#include "mldb/base/parallel.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/jml/utils/smart_ptr_utils.h"


namespace ML {

/*****************************************************************************/
/* UPDATE_WEIGHTS_PARALLEL                                                   */
/*****************************************************************************/

/** This class updates a weights matrix in response to a new stump having
    been learned.
*/

template<class Updater>
struct Update_Weights_Parallel : public Update_Weights<Updater> {
    explicit Update_Weights_Parallel(const Updater & updater = Updater())
        : Update_Weights<Updater>(updater)
    {
    }

    using Update_Weights<Updater>::updater;

    /** Apply the given stump to the given weights, given the training
        data.  This will update all of the weights. */
    void operator () (const Stump & stump,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & weights,
                      const Training_Data & data,
                      double & total)
    {
        /* First we split up the weights matrix into enough bands that we get a
           decent amount of work on each example.

           We want to update at least 10000 weights entries in each chunk,
           otherwise the overhead gets too high.
        */

        size_t nx = weights.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 4096;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / weights.shape()[1];

        total = 0.0;

        Lock lock;

        auto doJob = [&] (size_t x0, size_t x1)
            {
                double subtotal
                = this->Update_Weights<Updater>::operator ()
                    (stump, opt_info, cl_weight, weights, data,
                     x0, x1);

                Guard guard(lock);
                total += subtotal;
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }

    /** Apply the given classifier to the given weights, given the training
        data.  This will update all of the weights. */
    void operator () (const Classifier_Impl & classifier,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & weights,
                      const Training_Data & data,
                      double & total)
    {
        /* First we split up the weights matrix into enough bands that we get a
           decent amount of work on each example.

           We want to update at least 10000 weights entries in each chunk,
           otherwise the overhead gets too high.
        */
        
        size_t nx = weights.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 4096;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / weights.shape()[1];

        total = 0.0;

        Lock lock;
        
        auto doJob = [&] (size_t x0, size_t x1)
            {
                double subtotal
                = this->Update_Weights<Updater>::operator ()
                    (classifier, opt_info, cl_weight, weights, data,
                     x0, x1);

                Guard guard(lock);
                total += subtotal;
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }

};


/*****************************************************************************/
/* UPDATE_SCORES_PARALLEL                                                    */
/*****************************************************************************/

/** This class updates a scores matrix based on the output of the
    classifier, as well as calculating the accuracy of the classifier.
*/

template<class Output_Updater, class Scorer>
struct Update_Scores_Parallel
    : public Update_Scores<Output_Updater, Scorer> {

    typedef Update_Scores<Output_Updater, Scorer> Updater;

    explicit Update_Scores_Parallel(const Output_Updater & output_updater
                                        = Output_Updater())
        : Update_Scores<Output_Updater, Scorer>(output_updater)
    {
    }

    struct Job_Info {
        const Stump & stump;
        const Optimization_Info & opt_info;
        float cl_weight;
        boost::multi_array<float, 2> & output;
        const Training_Data & data;
        const distribution<float> & example_weights;

        Joint_Index index;
        const std::vector<Label> & labels;

        Lock lock;
        double & correct;

        const Updater & updater;

        Job_Info(const Stump & stump,
                 const Optimization_Info & opt_info,
                 float cl_weight,
                 boost::multi_array<float, 2> & output, const Training_Data & data,
                 const distribution<float> & example_weights,                 
                 double & correct,
                 const Updater & updater)
            : stump(stump), opt_info(opt_info),
              cl_weight(cl_weight), output(output),
              data(data), example_weights(example_weights),
              index(data.index().joint
                    (stump.predicted(), stump.split.feature(), BY_EXAMPLE,
                     IC_VALUE | IC_EXAMPLE)),
              labels(data.index().labels(stump.predicted())),
              correct(correct), updater(updater)
        {
            correct = 0.0;
        }
        
        /* Update a part of the data. */
        void update_range(int x_begin, int x_end)
        {
            double subtotal
                = updater(stump, opt_info,
                          cl_weight, output, data, example_weights,
                          x_begin, x_end);

            Guard guard(lock);
            correct += subtotal;
        }
    };
    
    struct Update_Job {
        Update_Job(std::shared_ptr<Job_Info> info, int n_begin,
                   int n_end)
            : info(info), n_begin(n_begin), n_end(n_end)
        {
        }

        std::shared_ptr<Job_Info> info;
        int n_begin, n_end;
        
        void operator () ()
        {
            info->update_range(n_begin, n_end);
        }
    };

    /** Apply the given stump to the given output, given the training
        data.  This will update all of the output. */
    void operator () (const Stump & stump,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & output,
                      const Training_Data & data,
                      const distribution<float> & example_weights,
                      double & correct)
    {
        /* Create the job info. */
        std::shared_ptr<Job_Info> info
            = make_sp(new Job_Info(stump, opt_info, cl_weight, output, data,
                                   example_weights, correct,
                                   *this));
        
        /* First we split up the output matrix into enough bands that we get a
           decent amount of work on each example.
           
           We want to update at least 10000 output entries in each chunk,
           otherwise the overhead gets too high.
        */

        size_t nx = output.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 2048;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / output.shape()[1];

        auto doJob = [&] (size_t x0, size_t x1)
            {
                Update_Job(info, x0, x1)();
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }

    struct Job_Info_Classifier {
        const Classifier_Impl & classifier;
        const Optimization_Info & opt_info;
        float cl_weight;
        boost::multi_array<float, 2> & output;
        const Training_Data & data;
        const distribution<float> & example_weights;

        const std::vector<Label> & labels;

        Lock lock;
        double & correct;

        const Updater & updater;

        Job_Info_Classifier(const Classifier_Impl & classifier,
                            const Optimization_Info & opt_info,
                            float cl_weight,
                            boost::multi_array<float, 2> & output,
                            const Training_Data & data,
                            const distribution<float> & example_weights,
                            double & correct,
                            const Updater & updater)
            : classifier(classifier), opt_info(opt_info),
              cl_weight(cl_weight), output(output),
              data(data), example_weights(example_weights),
              labels(data.index().labels(classifier.predicted())),
              correct(correct), updater(updater)
        {
            correct = 0.0;
        }
        
        /* Update a part of the data. */
        void update_range(int x_begin, int x_end)
        {
            double subtotal
                = updater(classifier, opt_info,
                          cl_weight, output, data, example_weights,
                          x_begin, x_end);

            Guard guard(lock);
            correct += subtotal;
        }
    };
    
    struct Update_Job_Classifier {
        Update_Job_Classifier(std::shared_ptr<Job_Info_Classifier> info,
                              int n_begin, int n_end)
            : info(info), n_begin(n_begin), n_end(n_end)
        {
        }

        std::shared_ptr<Job_Info_Classifier> info;
        int n_begin, n_end;
        
        void operator () ()
        {
            info->update_range(n_begin, n_end);
        }
    };

    /** Apply the given classifier to the given output, given the training
        data.  This will update all of the output. */
    void operator () (const Classifier_Impl & classifier,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & output,
                      const Training_Data & data,
                      const distribution<float> & example_weights,
                      double & correct)
    {
        /* Create the job info. */
        std::shared_ptr<Job_Info_Classifier> info
            = make_sp(new Job_Info_Classifier
                      (classifier, opt_info, cl_weight, output, data,
                       example_weights, correct, *this));
        
        /* First we split up the output matrix into enough bands that we get a
           decent amount of work on each example.
           
           We want to update at least 10000 output entries in each chunk,
           otherwise the overhead gets too high.
        */

        size_t nx = output.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 2048;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / output.shape()[1];

        auto doJob = [&] (size_t x0, size_t x1)
            {
                Update_Job_Classifier(info, x0, x1)();
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }

};

/*****************************************************************************/
/* UPDATE_WEIGHTS_AND_SCORES_PARALLEL                                        */
/*****************************************************************************/

/** This class updates a weights matrix in response to a new stump having
    been learned, and updates a scores matrix based on the output of the
    classifier, as well as calculating the accuracy of the classifier.
*/

template<class Weights_Updater, class Output_Updater, class Scorer>
struct Update_Weights_And_Scores_Parallel
    : Update_Weights_And_Scores<Weights_Updater, Output_Updater, Scorer> {
    typedef Update_Weights_And_Scores<Weights_Updater, Output_Updater, Scorer>
        Updater;
    explicit Update_Weights_And_Scores_Parallel
        (const Weights_Updater & weights_updater = Weights_Updater(),
         const Output_Updater & output_updater = Output_Updater())
            : Updater(weights_updater, output_updater)
    {
    }

    struct Job_Info {
        const Stump & stump;
        const Optimization_Info & opt_info;
        float cl_weight;
        boost::multi_array<float, 2> & weights;
        boost::multi_array<float, 2> & output;
        const Training_Data & data;
        const distribution<float> & example_weights;

        Lock lock;
        double & correct;
        double & total;

        const Updater & updater;

        Job_Info(const Stump & stump,
                 const Optimization_Info & opt_info,
                 float cl_weight,
                 boost::multi_array<float, 2> & weights,
                 boost::multi_array<float, 2> & output,
                 const Training_Data & data,
                 const distribution<float> & example_weights,                 
                 double & correct,
                 double & total,
                 const Updater & updater)
            : stump(stump), opt_info(opt_info),
              cl_weight(cl_weight), weights(weights),
              output(output),
              data(data), example_weights(example_weights),
              correct(correct), total(total), updater(updater)
        {
        }
        
        /* Update a part of the data. */
        void update_range(int x_begin, int x_end)
        {
#if 0
            using namespace std;
            cerr << stump.summary() << endl;
            for (unsigned i = x_begin;  i < x_begin + 10;  ++i) {
                if (i >= x_end) break;
                cerr << " " << weights[i][0];
            }
            cerr << endl;

            for (unsigned i = x_begin;  i < x_begin + 10;  ++i) {
                if (i >= x_end) break;
                cerr << " " << output[i][0];
            }
            cerr << endl;
#endif

            //boost::timer timer;
            double sub_correct = 0.0;
            double subtotal
                = updater(stump, opt_info, cl_weight, weights, output, data,
                          example_weights, sub_correct, x_begin, x_end);

#if 0
            cerr << "updating between " << x_begin << " and " << x_end
                //<< " with " << stump.summary()
                 << ": " << timer.elapsed() << endl;

            //if (timer.elapsed() > 0.1)
#endif

            Guard guard(lock);
            total += subtotal;
            correct += sub_correct;
        }
    };
    
    struct Update_Job {
        Update_Job(std::shared_ptr<Job_Info> info, int n_begin,
                   int n_end)
            : info(info), n_begin(n_begin), n_end(n_end)
        {
        }
        
        std::shared_ptr<Job_Info> info;
        int n_begin, n_end;
        
        void operator () ()
        {
            info->update_range(n_begin, n_end);
        }
    };

    /** Apply the given stump to the given output, given the training
        data.  This will update all of the output. */
    void operator () (const Stump & stump,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & weights,
                      boost::multi_array<float, 2> & output,
                      const Training_Data & data,
                      const distribution<float> & example_weights,
                      double & correct,
                      double & total)
    {
        /* Create the job info. */
        std::shared_ptr<Job_Info> info
            = make_sp(new Job_Info(stump, opt_info,
                                   cl_weight, weights, output, data,
                                   example_weights, correct, total,
                                   *this));
        
        /* First we split up the output matrix into enough bands that we get a
           decent amount of work on each example.
           
           We want to update at least 10000 output entries in each chunk,
           otherwise the overhead gets too high.
        */

        size_t nx = output.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 2048;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / output.shape()[1];

        auto doJob = [&] (size_t x0, size_t x1)
            {
                Update_Job(info, x0, x1)();
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }

    struct Job_Info_Classifier {
        const Classifier_Impl & classifier;
        const Optimization_Info & opt_info;
        float cl_weight;
        boost::multi_array<float, 2> & weights;
        boost::multi_array<float, 2> & output;
        const Training_Data & data;
        const distribution<float> & example_weights;

        Lock lock;
        double & correct;
        double & total;

        const Updater & updater;

        Job_Info_Classifier(const Classifier_Impl & classifier,
                            const Optimization_Info & opt_info,
                            float cl_weight,
                            boost::multi_array<float, 2> & weights,
                            boost::multi_array<float, 2> & output,
                            const Training_Data & data,
                            const distribution<float> & example_weights,                 
                            double & correct,
                            double & total,
                            const Updater & updater)
            : classifier(classifier), opt_info(opt_info),
              cl_weight(cl_weight), weights(weights),
              output(output),
              data(data), example_weights(example_weights),
              correct(correct), total(total), updater(updater)
        {
        }
        
        /* Update a part of the data. */
        void update_range(int x_begin, int x_end)
        {
            double sub_correct = 0.0;
            double subtotal
                = updater(classifier, opt_info,
                          cl_weight, weights, output, data,
                          example_weights, sub_correct, x_begin, x_end);

            Guard guard(lock);
            total += subtotal;
            correct += sub_correct;
        }
    };
    
    struct Update_Job_Classifier {
        Update_Job_Classifier(std::shared_ptr<Job_Info_Classifier> info, int n_begin,
                   int n_end)
            : info(info), n_begin(n_begin), n_end(n_end)
        {
        }
        
        std::shared_ptr<Job_Info_Classifier> info;
        int n_begin, n_end;
        
        void operator () ()
        {
            info->update_range(n_begin, n_end);
        }
    };

    /** Apply the given classifier to the given output, given the training
        data.  This will update all of the output. */
    void operator () (const Classifier_Impl & classifier,
                      const Optimization_Info & opt_info,
                      float cl_weight,
                      boost::multi_array<float, 2> & weights,
                      boost::multi_array<float, 2> & output,
                      const Training_Data & data,
                      const distribution<float> & example_weights,
                      double & correct,
                      double & total)
    {
        /* Create the job info. */
        std::shared_ptr<Job_Info_Classifier> info
            = make_sp(new Job_Info_Classifier
                      (classifier, opt_info, cl_weight, weights, output, data,
                       example_weights, correct, total, *this));
        
        /* First we split up the output matrix into enough bands that we get a
           decent amount of work on each example.
           
           We want to update at least 10000 output entries in each chunk,
           otherwise the overhead gets too high.
        */

        size_t nx = output.shape()[0];
        
        size_t ENTRIES_PER_CHUNK = 2048;
        size_t examples_per_chunk = ENTRIES_PER_CHUNK / output.shape()[1];

        auto doJob = [&] (size_t x0, size_t x1)
            {
                Update_Job_Classifier(info, x0, x1)();
            };

        MLDB::parallelMapChunked(0, nx, examples_per_chunk, doJob);
    }
};


} // namespace ML
