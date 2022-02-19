//#include "ext/dj_fft.hpp"

#if 0
template<typename Inputs>
static std::vector<float> autocorrelate(Inputs && inputs)
{
    size_t s = inputs.size();
    size_t n = 1;
    while (n < s) { n *= 2; }
    n *= 2;  // need to pad with zeros to avoid temporal aliasing

    if (n < 32)
        return {};

    double mean = 0.0;
    for (auto v: inputs)
        mean += v;
    mean /= inputs.size();

    double stdev = 0;
    for (auto v: inputs)
        stdev += (v - mean) * (v - mean);
    stdev = sqrt(stdev);

    cerr << "s = " << s << " n = " << n << " mean " << mean << " stdev " << stdev << endl;

    // Copy in, normalizing as we go to avoid constant factors blowing things up numerically
    std::vector<std::complex<float> > rescaled(n);
    std::transform(inputs.begin(), inputs.end(), rescaled.begin(), [&] (auto x) { return (x - mean) / stdev; });

    int range = 16;

    for (int i = -range;  i < (int)range;  ++i) {
        double total = 0.0;
        for (int j = 0;  j < (int)s;  ++j) {
            int k = j + i;
            if (k < 0 || k >= (int)s)
                continue;
            total += real(rescaled[j]) * real(rescaled[k]);
        }
        cerr << "autocorrolation at " << i << " is " << total << endl;
    }


    // fast correlation(x,x) = ifft(fft(padded x)*conj(fft(padded x)))
    // this is n log n rather than n^2 for the naive correlation
    // can be optimized further since we know it's in the real domain

    std::vector<std::complex<float> > fft = dj::fft1d(rescaled, dj::fft_dir::DIR_FWD);
    std::transform(fft.begin(), fft.end(), fft.begin(), [] (auto v) { return v * std::conj(v); });
    std::vector<std::complex<float> > autocorr = dj::fft1d(fft, dj::fft_dir::DIR_BWD);

    std::vector<float> result(n);
    std::transform(autocorr.begin(), autocorr.end(), result.begin(), [] (auto c) { return std::abs(c); });

    return result;
}
#endif

#if 0
    if (residuals.size() >= 32 && false) {
        auto analyzeCorrelation = [] (const std::vector<float> & correlation)
        {
            std::vector<std::pair<float, int> > values;
            for (size_t i = 0;  i < correlation.size();  ++i) {
                values.emplace_back(correlation[i], i);
            }

            std::sort(values.rbegin(), values.rend());

            for (size_t i = 0;  i < 20 && i < values.size();  ++i) {
                cerr << "  " << i << " " << values[i].second << " " << 1.0 * values[i].second / correlation.size()
                    << " " << values[i].first << endl;
            }
        };

        cerr << "residual autocorrelation" << endl;
        auto autocorr_r = autocorrelate(residuals);
        analyzeCorrelation(autocorr_r);

        cerr << "selector autocorrelation" << endl;
        auto autocorr_s = autocorrelate(selectors);
        analyzeCorrelation(autocorr_s);
    }
#endif

    //cerr << "selectors " << selectors << endl;
    //cerr << "residuals " << residuals << endl;

