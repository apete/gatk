package org.broadinstitute.hellbender.tools.copynumber;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hdf5.HDF5Library;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.CopyNumberProgramGroup;
import org.broadinstitute.hellbender.engine.spark.SparkCommandLineProgram;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.copynumber.annotation.GCBiasCorrector;
import org.broadinstitute.hellbender.tools.copynumber.coverage.denoising.svd.HDF5SVDReadCountPanelOfNormals;
import org.broadinstitute.hellbender.tools.copynumber.coverage.readcount.SimpleCountCollection;
import org.broadinstitute.hellbender.tools.copynumber.formats.CopyNumberStandardArgument;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.io.IOUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Tool to create a panel of normals (PoN) given read counts for control samples.  These should be either
 * HDF5 or TSV output files generated by {@link CollectFragmentCounts}.
 *
 * <p>
 *     The input read counts are first transformed to log2 fractional coverages and preprocessed
 *     according to specified filtering and imputation parameters.  Singular value decomposition (SVD)
 *     is then performed to find the first {@code numberOfEigensamples} principal components,
 *     which are stored in the PoN.  Some or all of these principal components can then be used for
 *     denoising case samples with {@link DenoiseReadCounts}; it is assumed that the principal components used
 *     represent systematic sequencing biases (rather than statistical noise).  Examining the singular values,
 *     which are also stored in the PoN, may be useful in determining the appropriate number
 *     of principal components to use for denoising.
 * </p>
 *
 * <p>
 *     If annotated intervals (produced by {@link AnnotateIntervals} are provided, explicit GC-bias correction
 *     will be performed by {@link GCBiasCorrector} before filtering and SVD.  GC-content information for the
 *     intervals will be stored in the PoN and used to perform explicit GC-bias correction identically when denoising
 *     case samples.  Note that if annotated intervals are not provided, it is still likely that GC-bias correction
 *     is implicitly performed by the SVD denoising (i.e., some of the principal components arise from GC bias).
 * </p>
 *
 * <h3>Examples</h3>
 *
 * <pre>
 * gatk-launch --javaOptions "-Xmx4g" CreateReadCountPanelOfNormals \
 *   --input normal_1.readCounts.hdf5 \
 *   --input normal_2.readCounts.hdf5 \
 *   ... \
 *   --output panel_of_normals.pon.hdf5
 * </pre>
 *
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
@CommandLineProgramProperties(
        summary = "Create a panel of normals for read-count denoising given the read counts for samples in the panel.",
        oneLineSummary = "Create a panel of normals for read-count denoising.",
        programGroup = CopyNumberProgramGroup.class
)
@DocumentedFeature
public final class CreateReadCountPanelOfNormals extends SparkCommandLineProgram {
    private static final long serialVersionUID = 1L;

    //parameter names
    public static final String MINIMUM_INTERVAL_MEDIAN_PERCENTILE_LONG_NAME = "minimumIntervalMedianPercentile";
    public static final String MAXIMUM_ZEROS_IN_SAMPLE_PERCENTAGE_LONG_NAME = "maximumZerosInSamplePercentage";
    public static final String MAXIMUM_ZEROS_IN_INTERVAL_PERCENTAGE_LONG_NAME = "maximumZerosInIntervalPercentage";
    public static final String EXTREME_SAMPLE_MEDIAN_PERCENTILE_LONG_NAME = "extremeSampleMedianPercentile";
    public static final String IMPUTE_ZEROS_LONG_NAME = "doImputeZeros";
    public static final String EXTREME_OUTLIER_TRUNCATION_PERCENTILE_LONG_NAME = "extremeOutlierTruncationPercentile";

    //default values for filtering
    private static final double DEFAULT_MINIMUM_INTERVAL_MEDIAN_PERCENTILE = 10.0;
    private static final double DEFAULT_MAXIMUM_ZEROS_IN_SAMPLE_PERCENTAGE = 5.0;
    private static final double DEFAULT_MAXIMUM_ZEROS_IN_INTERVAL_PERCENTAGE = 5.0;
    private static final double DEFAULT_EXTREME_SAMPLE_MEDIAN_PERCENTILE = 2.5;
    private static final boolean DEFAULT_DO_IMPUTE_ZEROS = true;
    private static final double DEFAULT_EXTREME_OUTLIER_TRUNCATION_PERCENTILE = 0.1;

    private static final int DEFAULT_NUMBER_OF_EIGENSAMPLES = 20;

    @Argument(
            doc = "Input read-count files containing integer read counts in genomic intervals for all samples in the panel of normals.  " +
                    "Intervals must be identical and in the same order for all samples.",
            fullName = StandardArgumentDefinitions.INPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.INPUT_SHORT_NAME,
            minElements = 1
    )
    private List<File> inputReadCountFiles = new ArrayList<>();

    @Argument(
            doc = "Input annotated-interval file containing annotations for GC content in genomic intervals (output of AnnotateIntervals).  " +
                    "If provided, explicit GC correction will be performed before performing SVD.  " +
                    "Intervals must be identical to and in the same order as those in the input read-count files.",
            fullName = CopyNumberStandardArgument.ANNOTATED_INTERVALS_FILE_LONG_NAME,
            shortName = CopyNumberStandardArgument.ANNOTATED_INTERVALS_FILE_SHORT_NAME,
            optional = true
    )
    private File annotatedIntervalsFile = null;

    @Argument(
            doc = "Output file name for the panel of normals.  " +
                    "Output is given in the HDF5 file format.  Contents can be viewed with the hdfview program.",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private File outputPanelOfNormalsFile;

    @Argument(
            doc = "Genomic intervals with a median (across samples) of fractional coverage (optionally corrected for GC bias) " +
                    "below this percentile are filtered out.  " +
                    "(This is the first filter applied.)",
            fullName = MINIMUM_INTERVAL_MEDIAN_PERCENTILE_LONG_NAME,
            minValue = 0.,
            maxValue = 100.,
            optional = true
    )
    private double minimumIntervalMedianPercentile = DEFAULT_MINIMUM_INTERVAL_MEDIAN_PERCENTILE;

    @Argument(
            doc = "Samples with a fraction of zero-coverage genomic intervals above this percentage are filtered out.  " +
                    "(This is the second filter applied.)",
            fullName = MAXIMUM_ZEROS_IN_SAMPLE_PERCENTAGE_LONG_NAME,
            minValue = 0.,
            maxValue = 100.,
            optional = true
    )
    private double maximumZerosInSamplePercentage = DEFAULT_MAXIMUM_ZEROS_IN_SAMPLE_PERCENTAGE;

    @Argument(
            doc = "Genomic intervals with a fraction of zero-coverage samples above this percentage are filtered out.  " +
                    "(This is the third filter applied.)",
            fullName = MAXIMUM_ZEROS_IN_INTERVAL_PERCENTAGE_LONG_NAME,
            minValue = 0.,
            maxValue = 100.,
            optional = true
    )
    private double maximumZerosInIntervalPercentage = DEFAULT_MAXIMUM_ZEROS_IN_INTERVAL_PERCENTAGE;

    @Argument(
            doc = "Samples with a median (across genomic intervals) of fractional coverage normalized by genomic-interval medians  " +
                    "below this percentile or above the complementary percentile are filtered out.  " +
                    "(This is the fourth filter applied.)",
            fullName = EXTREME_SAMPLE_MEDIAN_PERCENTILE_LONG_NAME,
            minValue = 0.,
            maxValue = 50.,
            optional = true
    )
    private double extremeSampleMedianPercentile = DEFAULT_EXTREME_SAMPLE_MEDIAN_PERCENTILE;

    @Argument(
            doc = "If true, impute zero-coverage values as the median of the non-zero values in the corresponding interval.  " +
                    "(This is applied after all filters.)",
            fullName = IMPUTE_ZEROS_LONG_NAME,
            optional = true
    )
    private boolean doImputeZeros = DEFAULT_DO_IMPUTE_ZEROS;

    @Argument(
            doc = "Fractional coverages normalized by genomic-interval medians that are " +
                    "below this percentile or above the complementary percentile are set to the corresponding percentile value.  " +
                    "(This is applied after all filters and imputation.)",
            fullName = EXTREME_OUTLIER_TRUNCATION_PERCENTILE_LONG_NAME,
            minValue = 0.,
            maxValue = 50.,
            optional = true
    )
    private double extremeOutlierTruncationPercentile = DEFAULT_EXTREME_OUTLIER_TRUNCATION_PERCENTILE;

    @Argument(
            doc = "Number of eigensamples to use for truncated SVD and to store in the panel of normals.  " +
                    "The number of samples retained after filtering will be used instead if it is smaller than this.",
            fullName = CopyNumberStandardArgument.NUMBER_OF_EIGENSAMPLES_LONG_NAME,
            shortName = CopyNumberStandardArgument.NUMBER_OF_EIGENSAMPLES_SHORT_NAME,
            minValue = 1,
            optional = true
    )
    private int numEigensamplesRequested = DEFAULT_NUMBER_OF_EIGENSAMPLES;

    @Override
    protected void runPipeline(final JavaSparkContext ctx) {
        if (!new HDF5Library().load(null)) {  //Note: passing null means using the default temp dir.
            throw new UserException.HardwareFeatureException("Cannot load the required HDF5 library. " +
                    "HDF5 is currently supported on x86-64 architecture and Linux or OSX systems.");
        }

        //validate parameters and parse optional parameters
        validateArguments();

        //get sample filenames
        final List<String> sampleFilenames = inputReadCountFiles.stream().map(File::getAbsolutePath).collect(Collectors.toList());

        //get intervals from the first read-count file to use as the canonical list of intervals
        //(this file is read again below, which is slightly inefficient but is probably not worth the extra code)
        final List<SimpleInterval> intervals = getIntervalsFromFirstReadCountFile(logger, inputReadCountFiles);

        //get GC content (null if not provided)
        final double[] intervalGCContent = GCBiasCorrector.validateIntervalGCContent(intervals, annotatedIntervalsFile);

        //validate input read-count files (i.e., check intervals and that only integer counts are contained)
        //and aggregate as a RealMatrix with dimensions numIntervals x numSamples
        final RealMatrix readCountMatrix = constructReadCountMatrix(logger, inputReadCountFiles, intervals);

        //create the PoN
        logger.info("Creating the panel of normals...");
        HDF5SVDReadCountPanelOfNormals.create(outputPanelOfNormalsFile, getCommandLine(),
                readCountMatrix, sampleFilenames, intervals, intervalGCContent,
                minimumIntervalMedianPercentile, maximumZerosInSamplePercentage, maximumZerosInIntervalPercentage,
                extremeSampleMedianPercentile, doImputeZeros, extremeOutlierTruncationPercentile, numEigensamplesRequested, ctx);

        logger.info("Panel of normals successfully created.");
    }

    private void validateArguments() {
        Utils.validateArg(inputReadCountFiles.size() == new HashSet<>(inputReadCountFiles).size(),
                "List of input read-count files cannot contain duplicates.");
        inputReadCountFiles.forEach(IOUtils::canReadFile);
        if (numEigensamplesRequested > inputReadCountFiles.size()) {
            logger.warn(String.format("Number of eigensamples (%d) is greater than the number of input samples (%d); " +
                            "the number of samples retained after filtering will be used instead.",
                    numEigensamplesRequested, inputReadCountFiles.size()));
        }
    }

    private static List<SimpleInterval> getIntervalsFromFirstReadCountFile(final Logger logger,
                                                                           final List<File> inputReadCountFiles) {
        final File firstReadCountFile = inputReadCountFiles.get(0);
        logger.info(String.format("Retrieving intervals from first read-count file (%s)...", firstReadCountFile));
        final SimpleCountCollection readCounts = SimpleCountCollection.read(firstReadCountFile);
        return readCounts.getIntervals();
    }

    private static RealMatrix constructReadCountMatrix(final Logger logger,
                                                       final List<File> inputReadCountFiles,
                                                       final List<SimpleInterval> intervals) {
        logger.info("Validating and aggregating input read-count files...");
        final int numSamples = inputReadCountFiles.size();
        final int numIntervals = intervals.size();
        final RealMatrix readCountMatrix = new Array2DRowRealMatrix(numSamples, numIntervals);
        final ListIterator<File> inputReadCountFilesIterator = inputReadCountFiles.listIterator();
        while (inputReadCountFilesIterator.hasNext()) {
            final int sampleIndex = inputReadCountFilesIterator.nextIndex();
            final File inputReadCountFile = inputReadCountFilesIterator.next();
            logger.info(String.format("Aggregating read-count file %s (%d / %d)", inputReadCountFile, sampleIndex + 1, numSamples));
            final SimpleCountCollection readCounts = SimpleCountCollection.read(inputReadCountFile);
            Utils.validateArg(readCounts.getIntervals().equals(intervals),
                    String.format("Intervals for read-count file %s do not match those in other read-count files.", inputReadCountFile));
            readCountMatrix.setRow(sampleIndex, readCounts.getCounts());
        }
        return readCountMatrix;
    }
}
