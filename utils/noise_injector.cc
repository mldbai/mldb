#include "noise_injector.h"
#include <cstdlib>
#include <random>

/*****************************************************************************/
/* NOISE INJECTOR                                                            */
/*****************************************************************************/

using namespace std;

namespace MLDB {

double 
NoiseInjector::
rand_uniform() const {
    // http://stackoverflow.com/questions/21237905/how-do-i-generate-thread-safe-uniform-random-numbers
    std::random_device rd;
    static thread_local std::mt19937 gen(rd());

    std::uniform_real_distribution<> dis(0, 1);
    return dis(gen) - 0.5;
}

double
NoiseInjector::
sample_laplace() const {
    // https://en.wikipedia.org/wiki/Laplace_distribution#Generating_random_variables_according_to_the_Laplace_distribution
    double U = rand_uniform();
    return mu - (b * sgn(U)) * log(1-2*abs(U));
}

int64_t 
NoiseInjector::
add_noise(int64_t count, int64_t max) const {
    double noisy_count = count + sample_laplace();
    if(noisy_count < 0)   return 0;
    if(noisy_count > max) return max;
    return round(noisy_count);
}

} // namespace MLDB
