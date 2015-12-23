package org.broadinstitute.hellbender.tools.exome.allelefraction;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.broadinstitute.hellbender.tools.exome.AllelicCount;
import org.broadinstitute.hellbender.tools.exome.SegmentedModel;
import org.broadinstitute.hellbender.utils.mcmc.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static org.broadinstitute.hellbender.utils.MathUtils.logFactorial;

/**
 * Given a {@link org.broadinstitute.hellbender.tools.exome.SegmentedModel} and counts of alt and ref reads over a list of het sites,
 * infers the minor allele fraction of each segment.  For example, a segment
 * with (alt,ref) counts (10,90), (11,93), (88,12), (90,10) probably has a minor allele fraction
 * somewhere around 0.1.  The model takes into account allelic bias due to mapping etc. by learning
 * a global gamma distribution on allelic bias ratios.
 *
 * For a given het locus, we define the bias ratio to be the expected ratio of
 * mapped ref reads to mapped alt reads given equal amounts of DNA (that is, given
 * a germline het).  The model learns a common gamma distribution:
 *      bias ratio ~ Gamma(mu*beta, beta)
 * where mu is the global mean of bias ratios (probably slightly larger than 1.00), and
 * beta is the rate parameter such that variance = mu/beta.
 *
 * Each segment has a minor allele fraction f, and for each het within the locus
 * the number of alt reads is drawn from a binomial distribution with total count
 * n = #alt reads + #ref reads and alt probability f/(f + (1-f)*bias ratio) if the
 * locus is alt minor and (1-f)/(1-f + f*bias ratio) if the locus is ref minor.
 *
 * Conceptually, the model contains latent variable corresponding to the bias ratio
 * and an indicator for alt minor/ref minor at each het locus.  However, we integrate them
 * out and the MCMC model below only contains the minor allele fractions and
 * the three hyperparameters of the model: the two parameters of the gamma distribution
 * along with the global outlier probability.
 *
 * See docs/AllelicCapSeg/ACS-methods.pdf for a thorough description of the model.
 *
 * @author David Benjamin &lt;davidben@broadinstitute.org&gt;
 */
public final class AlleleFractionModeller {
    private final SegmentedModel segmentedModel;
    private final ParameterizedModel<AlleleFractionState, AlleleFractionData> model;
    private final List<Double> meanBiasSamples = new ArrayList<>();
    private final List<Double> biasVarianceSamples = new ArrayList<>();
    private final List<Double> outlierProbabilitySamples = new ArrayList<>();
    private final List<AlleleFractionState.MinorFractions> minorFractionsSamples = new ArrayList<>();
    private final int numSegments;

    // INNER SAMPLER CLASSES -------------------------------------------------------------------------------------------
    // sample mean bias
    @VisibleForTesting
    protected static final class  MeanBiasSampler implements Sampler<Double, AlleleFractionState, AlleleFractionData> {
        private static final double INITIAL_MEAN_BIAS_STEP_SIZE = 0.01;
        private final AdaptiveMetropolisSampler sampler;

        public MeanBiasSampler(final double initialMeanBias) {
            sampler = new AdaptiveMetropolisSampler(initialMeanBias, INITIAL_MEAN_BIAS_STEP_SIZE, 0, Double.POSITIVE_INFINITY);
        }

        public Double sample(final RandomGenerator rng, final AlleleFractionState state, final AlleleFractionData data) {
            return sampler.sample(x -> logLikelihood(state.shallowCopyWithProposedMeanBias(x), data));
        }
    }

    // sample bias variance
    @VisibleForTesting
    protected static final class  BiasVarianceSampler implements Sampler<Double, AlleleFractionState, AlleleFractionData> {
        private static final double INITIAL_BIAS_VARIANCE_STEP_SIZE = 0.001;
        private final AdaptiveMetropolisSampler sampler;

        public BiasVarianceSampler(final double initialBiasVariance) {
            sampler = new AdaptiveMetropolisSampler(initialBiasVariance, INITIAL_BIAS_VARIANCE_STEP_SIZE, 0, Double.POSITIVE_INFINITY);
        }

        public Double sample(final RandomGenerator rng, final AlleleFractionState state, final AlleleFractionData data) {
            return sampler.sample(x -> logLikelihood(state.shallowCopyWithProposedBiasVariance(x), data));
        }
    }

    // sample outlier probability
    @VisibleForTesting
    protected static final class  OutlierProbabilitySampler implements Sampler<Double, AlleleFractionState, AlleleFractionData> {
        private static final double INITIAL_OUTLIER_PROBABILITY_STEP_SIZE = 0.01;
        private static final double MAX_OUTLIER_PROBABILITY = 0.1;
        private final AdaptiveMetropolisSampler sampler;

        public OutlierProbabilitySampler(final double initialOutlierProbability) {
            sampler = new AdaptiveMetropolisSampler(initialOutlierProbability, INITIAL_OUTLIER_PROBABILITY_STEP_SIZE, 0, MAX_OUTLIER_PROBABILITY);
        }

        public Double sample(final RandomGenerator rng, final AlleleFractionState state, final AlleleFractionData data) {
            return sampler.sample(x -> logLikelihood(state.shallowCopyWithProposedOutlierProbability(x), data));
        }
    }

    // sample minor fraction of a single segment
    private static final class PerSegmentMinorFractionSampler implements Sampler<Double, AlleleFractionState, AlleleFractionData> {
        private static final double INITIAL_MINOR_FRACTION_STEP_SIZE = 0.05;
        private static final double MAX_MINOR_FRACTION = 0.5;   //by definition!
        private final AdaptiveMetropolisSampler sampler;

        private final int segmentIndex;

        public PerSegmentMinorFractionSampler(final int segmentIndex, final double initialMinorFraction) {
            sampler = new AdaptiveMetropolisSampler(initialMinorFraction, INITIAL_MINOR_FRACTION_STEP_SIZE, 0, MAX_MINOR_FRACTION);
            this.segmentIndex = segmentIndex;
        }

        public Double sample(final RandomGenerator rng, final AlleleFractionState state, final AlleleFractionData data) {
            if (data.numHetsInSegment(segmentIndex) == 0) {
                return Double.NaN;
            }
            return sampler.sample(x -> {
                final AlleleFractionState proposal = makeSingleSegmentState(state.meanBias(),
                        state.biasVariance(), state.outlierProbability(), x);
                return segmentLogLikelihood(proposal, 0, data.countsInSegment(segmentIndex));
            });
        }
    }

    // sample minor fractions of all segments
    @VisibleForTesting
    protected static final class MinorFractionsSampler implements Sampler<AlleleFractionState.MinorFractions, AlleleFractionState, AlleleFractionData> {
        private final List<PerSegmentMinorFractionSampler> perSegmentSamplers = new ArrayList<>();
        private final int numSegments;

        public MinorFractionsSampler(final AlleleFractionState.MinorFractions initialMinorFractions) {
            this.numSegments = initialMinorFractions.size();
            for (int segment = 0; segment < numSegments; segment++) {
                perSegmentSamplers.add(new PerSegmentMinorFractionSampler(segment, initialMinorFractions.get(segment)));
            }
        }

        public AlleleFractionState.MinorFractions sample(final RandomGenerator rng, final AlleleFractionState state, final AlleleFractionData data) {
            return new AlleleFractionState.MinorFractions(perSegmentSamplers.stream()
                    .map(sampler -> sampler.sample(rng, state, data)).collect(Collectors.toList()));
        }
    }

    // END OF INNER SAMPLER CLASSES SECTION ----------------------------------------------------------------------------

    public AlleleFractionModeller(final SegmentedModel segmentedModel) {
        this.segmentedModel = segmentedModel;
        final AlleleFractionData data = new AlleleFractionData(segmentedModel);
        numSegments = data.numSegments();
        final AlleleFractionState initialState = new AlleleFractionInitializer(data).getInitializedState();

        final Sampler<Double, AlleleFractionState, AlleleFractionData> meanBiasSampler =
                new MeanBiasSampler(initialState.meanBias());
        final Sampler<Double, AlleleFractionState, AlleleFractionData> biasVarianceSampler =
                new BiasVarianceSampler(initialState.biasVariance());
        final Sampler<Double, AlleleFractionState, AlleleFractionData> outlierProbabilitySampler =
                new OutlierProbabilitySampler(initialState.outlierProbability());
        final Sampler<AlleleFractionState.MinorFractions, AlleleFractionState, AlleleFractionData> minorFractionsSampler =
                new MinorFractionsSampler(initialState.minorFractions());

        model = new ParameterizedModel.GibbsBuilder<>(initialState, data, AlleleFractionState.class)
                .addParameterSampler(AlleleFractionState.MEAN_BIAS_NAME, meanBiasSampler, Double.class)
                .addParameterSampler(AlleleFractionState.BIAS_VARIANCE_NAME, biasVarianceSampler, Double.class)
                .addParameterSampler(AlleleFractionState.P_OUTLIER_NAME, outlierProbabilitySampler, Double.class)
                .addParameterSampler(AlleleFractionState.MINOR_FRACTIONS_NAME, minorFractionsSampler, AlleleFractionState.MinorFractions.class)
                .build();
    }

    /**
     * Compute the log-likelihood of a alt reads and r ref reads given minor fraction f and gamma hyperparameters
     * (specifying the distribution on allelic biases) mu (mean) and beta (rate = mean/variance) and given
     * an alt minor, ref minor, or outlier indicator state.  Note that this is a partially collapsed log-likelihood in that the
     * latent variable corresponding to the allelic bias at this site has been marginalized out but the indicator
     * variable has not been marginalized out.
     * <p>
     * See docs/ACS-methods.pdf for derivation.
     * <p>
     * Finally, note that this is a static method and does not get mu, beta, and minorFraction from an AlleleFractionState object
     * We need such functionality because MCMC evaluates the likelihood under proposed parameter changes.
     *
     * @param state allele fraction state
     * @param segment index of segment containijng this het site
     * @param count AllelicCount of alt and ref reads
     * @param indicator the hidden state (alt minor / ref minor / outlier)
     *
     * @return if indicator == ALT_MINOR:
     * <p>
     * log { [beta^alpha / Gamma(alpha)][(1-pi)/2] * int_{0 to infty} f^a * (1-f)^r * lambda^(alpha + r - 1) * exp(-beta*lambda)/(f + (1-f)*lambda)^n d lambda }
     * <p>
     * if indicator == REF_MINOR, same as ALT_MINOR but with f <--> 1 - f
     * <p>
     * if indicator == OUTLIER log {pi * a!r!/(n+1)!}
     * <p>
     * where alpha = mu*beta and n = a + r
     */
    public static double hetLogLikelihood(final AlleleFractionState state, final int segment, final AllelicCount count, final AlleleFractionIndicator indicator) {
        return logIntegralOfPerHetLikelihood(state, segment, count, indicator, 0);
    }

    /**
     * the log likelihood summed (marginalized) over indicator states, which we use in the fully collapsed model
     * in which latent variables (bias and indicator) are marginalized out
     *
     * @param state allele fraction state
     * @param segment index of segment containijng this het site
     * @param count AllelicCount of alt and ref reads
     * @return the log of the likelihood at this het site, marginalzied over indicator states.
     */
    public static double collapsedHetLogLikelihood(final AlleleFractionState state, final int segment, final AllelicCount count) {
        return logSumLog(hetLogLikelihood(state, segment, count, AlleleFractionIndicator.ALT_MINOR),
                hetLogLikelihood(state, segment, count, AlleleFractionIndicator.REF_MINOR),
                hetLogLikelihood(state, segment, count, AlleleFractionIndicator.OUTLIER));
    }

    /**
     * the log-likelihood of all the hets in a segment
     *
     * @param state allele fraction state
     * @param segment index of segment containijng this het site
     * @param counts AllelicCount of alt and ref reads in this segment
     * @return the sum of log-likelihoods over all het sites in a segment
     */
    public static double segmentLogLikelihood(final AlleleFractionState state, final int segment, final Collection<AllelicCount> counts) {
        return counts.stream().mapToDouble(c -> collapsedHetLogLikelihood(state, segment, c)).sum();
    }

    /**
     * the total log likelihood of all segments
     * @param state current state
     * @param data data
     * @return sum of log likelihoods of all segments
     */
    public static double logLikelihood(final AlleleFractionState state, final AlleleFractionData data) {
        return IntStream.range(0, data.numSegments()).mapToDouble(s -> segmentLogLikelihood(state, s, data.countsInSegment(s))).sum();
    }

    /**
     * Compute the kth moment of allelic bias with respect to the het likelihood integrand, that is
     * int [lambda^order * integrand d lambda]/int [integrand d lambda]
     * As we have a private method for calculating the numerator and denominator integrands, this is easy
     *
     * @param state allele fraction state
     * @param segment index of segment containijng this het site
     * @param count AllelicCount of alt and ref reads
     * @param order the desired moment
     * @return kth moment of allelic bias with respect to the het likelihood integrand
     */
    public static double biasPosteriorMoment(final AlleleFractionState state, final int segment, final AllelicCount count,
                                             final AlleleFractionIndicator indicator, final int order) {
        final double logNumerator = logIntegralOfPerHetLikelihood(state, segment, count, indicator, order);
        final double logDenominator = logIntegralOfPerHetLikelihood(state, segment, count, indicator, 0);
        return Math.exp(logNumerator - logDenominator);
    }

    /**
     * Adds {@code numSamples - numBurnIn} Markov-Chain Monte-Carlo samples of the parameter posteriors (generated using
     * Gibbs sampling) to the collections held internally.  The current {@link AlleleFractionState} held internally is used
     * to initialize the Markov Chain.
     * @param numSamples    total number of samples per posterior
     * @param numBurnIn     number of burn-in samples to discard
     */
    public void fitMCMC(final int numSamples, final int numBurnIn) {
        //run MCMC
        final GibbsSampler<AlleleFractionState, AlleleFractionData> gibbsSampler = new GibbsSampler<>(numSamples, model);
        gibbsSampler.runMCMC();

        //update posterior samples
        meanBiasSamples.addAll(gibbsSampler.getSamples(AlleleFractionState.MEAN_BIAS_NAME, Double.class, numBurnIn));
        biasVarianceSamples.addAll(gibbsSampler.getSamples(AlleleFractionState.BIAS_VARIANCE_NAME, Double.class, numBurnIn));
        outlierProbabilitySamples.addAll(gibbsSampler.getSamples(AlleleFractionState.P_OUTLIER_NAME, Double.class, numBurnIn));
        minorFractionsSamples.addAll(gibbsSampler.getSamples(AlleleFractionState.MINOR_FRACTIONS_NAME, AlleleFractionState.MinorFractions.class, numBurnIn));
    }

    /**
     * Returns the {@link SegmentedModel} held internally.
     * @return the {@link SegmentedModel} held internally
     */
    public SegmentedModel getSegmentedModel() {
        return segmentedModel;
    }

    public List<Double> getmeanBiasSamples() {
        return Collections.unmodifiableList(meanBiasSamples);
    }
    public List<Double> getBiasVarianceSamples() {
        return Collections.unmodifiableList(biasVarianceSamples);
    }
    public List<Double> getOutlierProbabilitySamples() { return Collections.unmodifiableList(outlierProbabilitySamples); }
    public List<AlleleFractionState.MinorFractions> getMinorFractionsSamples() { return Collections.unmodifiableList(minorFractionsSamples); }

    /**
     * Returns a list of {@link PosteriorSummary} elements summarizing the minor-allele-fraction posterior for each segment.
     * Should only be called after {@link AlleleFractionModeller#fitMCMC(int, int)} has been called.
     * @return  list of {@link PosteriorSummary} elements summarizing the minor-allele-fraction posterior for each segment
     */
    public List<PosteriorSummary> getMinorAlleleFractionsPosteriorSummaries() {
        final int numSegments = segmentedModel.getSegments().size();
        final List<PosteriorSummary> posteriorSummaries = new ArrayList<>(numSegments);
        for (int segment = 0; segment < numSegments; segment++) {
            final int j = segment;
            final double[] minorFractionSamples =
                    Doubles.toArray(minorFractionsSamples.stream().map(s -> s.get(j))
                            .collect(Collectors.toList()));
            final double posteriorMean = new Mean().evaluate(minorFractionSamples);
            final double posteriorStandardDeviation = new StandardDeviation().evaluate(minorFractionSamples);
            posteriorSummaries.add(new PosteriorSummary(posteriorMean, posteriorStandardDeviation));
        }
        return posteriorSummaries;
    }

    public List<List<Double>> getMinorFractionSamplesBySegment() {
        final List<List<Double>> result = new ArrayList<>();
        for (int segment = 0; segment < numSegments; segment++) {
            final List<Double> thisSegment = new ArrayList<>();
            for (final AlleleFractionState.MinorFractions sample : minorFractionsSamples) {
                thisSegment.add(sample.get(segment));
            }
            result.add(thisSegment);
        }
        return result;
    }

    //compute log(e^a + e^b + e^c) = log(e^M [e^(a-M) + e^(b-M) + e^(c-M)]) where M = max(a,b,c)
    private static double logSumLog(final double a, final double b, final double c) {
        final double maxValue = Doubles.max(a, b, c);
        return maxValue + Math.log(Math.exp(a-maxValue) + Math.exp(b-maxValue) + Math.exp(c-maxValue));
    }

    /**
     * Log of the integral the unmarginalized per-het likelihood over the latent bias ratio variable.  For use in calculating
     * posterior moments we also multiply the integrand by bias raised to an integer power.  This order can be set to zero to obtain
     * the marginalized log-likelihood.
     * <p>
     * See docs/ACS-methods.pdf for derivation.
     * <p>
     * Finally, note that this is a static method and does not get mu, beta, and minorFraction from an AlleleFractionState object
     * We need such functionality because MCMC evaluates the likelihood under proposed parameter changes.
     *
     * @param state allele fraction state
     * @param segment index of segment containijng this het site
     * @param count AllelicCount of alt and ref reads
     * @param indicator the hidden state (alt minor / ref minor / outlier)
     * @param order the integer power of bias with which to multiply the integrand
     *
     * @return if indicator == ALT_MINOR:
     * <p>
     * log { [beta^alpha / Gamma(alpha)][(1-pi)/2] * int_{0 to infty}
     * f^a * (1-f)^r * lambda^order * lambda^(alpha + r - 1) * exp(-beta*lambda)/(f + (1-f)*lambda)^n d lambda }
     * <p>
     * if indicator == REF_MINOR, same as ALT_MINOR but with f <--> 1 - f
     * <p>
     * if indicator == OUTLIER log {pi * a!r!/(n+1)!}
     * <p>
     * where alpha = mu*beta and n = a + r
     */
    private static double logIntegralOfPerHetLikelihood(final AlleleFractionState state, final int segment,
            final AllelicCount count, final AlleleFractionIndicator indicator, final int order) {
        final double mu = state.meanBias();
        final double beta = state.meanBias() / state.biasVariance();
        final double alpha = mu * beta;
        final double pi = state.outlierProbability();
        final double minorFraction = state.minorFractionInSegment(segment);
        final int a = count.getAltReadCount();
        final int r = count.getRefReadCount();

        if (indicator == AlleleFractionIndicator.OUTLIER) {
            return log(pi) + logFactorial(a) + logFactorial(r) - logFactorial(a + r + 1);
        } else {
            final double f = indicator == AlleleFractionIndicator.ALT_MINOR ? minorFraction : 1 - minorFraction;
            final int n = a + r;
            final double w = (1 - f) * (a - alpha - order + 1) + beta * f;
            final double lambda0 = (sqrt(w * w + 4 * beta * f * (1 - f) * (r + alpha + order - 1)) - w) / (2 * beta * (1 - f));
            final double y = (1 - f)/(f + (1 - f) * lambda0);
            final double kappa = n * y * y - (r + alpha + order - 1) / (lambda0 * lambda0);
            final double rho = 1 - kappa * lambda0 * lambda0;
            final double tau = -kappa * lambda0;
            final double logc = alpha*log(beta) - Gamma.logGamma(alpha) + a * log(f) + r * log(1 - f)
                    + (r + alpha + order - rho) * log(lambda0) + (tau - beta) * lambda0 - n * log(f + (1 - f) * lambda0);

            return log((1 - pi) / 2) + logc + Gamma.logGamma(rho) - rho * log(tau);
        }
    }

    public static AlleleFractionState makeSingleSegmentState(final double meanBias, final double biasVariance,
                                                             final double outlierProbability, final double minorFraction) {
        return new AlleleFractionState(meanBias, biasVariance, outlierProbability,
                new AlleleFractionState.MinorFractions(Arrays.asList(minorFraction)));
    }
}