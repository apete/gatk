package org.broadinstitute.hellbender.tools.spark.pipelines;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.spark.storage.StorageLevel;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSink;
import org.broadinstitute.hellbender.tools.HaplotypeCallerSpark;
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCallerArgumentCollection;
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCallerEngine;
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.ReferenceConfidenceMode;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SerializableFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkPipelineProgramGroup;
import org.broadinstitute.hellbender.engine.ReadContextData;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.AddContextDataToReadSpark;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.JoinStrategy;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.ApplyBQSRUniqueArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.transforms.ApplyBQSRSparkFn;
import org.broadinstitute.hellbender.tools.spark.transforms.BaseRecalibratorSparkFn;
import org.broadinstitute.hellbender.tools.spark.transforms.markduplicates.MarkDuplicatesSpark;
import org.broadinstitute.hellbender.tools.walkers.bqsr.BaseRecalibrator;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadCoordinateComparator;
import org.broadinstitute.hellbender.utils.read.markduplicates.MarkDuplicatesScoringStrategy;
import org.broadinstitute.hellbender.utils.read.markduplicates.OpticalDuplicateFinder;
import org.broadinstitute.hellbender.utils.recalibration.BaseRecalibrationEngine;
import org.broadinstitute.hellbender.utils.recalibration.RecalibrationArgumentCollection;
import org.broadinstitute.hellbender.utils.recalibration.RecalibrationReport;
import org.broadinstitute.hellbender.utils.spark.SparkUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;


@CommandLineProgramProperties(
        summary = "Takes aligned reads (likely from BWA) and runs MarkDuplicates and BQSR. The final result is analysis-ready reads.",
        oneLineSummary = "Takes aligned reads (likely from BWA) and runs MarkDuplicates and BQSR. The final result is analysis-ready reads",
        usageExample = "ReadsPipelineSpark -I single.bam -R referenceURL -knownSites variants.vcf -O file:///tmp/output.bam",
        programGroup = SparkPipelineProgramGroup.class
)

/**
 * ReadsPipelineSpark is our standard pipeline that takes aligned reads (likely from BWA) and runs MarkDuplicates
 * and BQSR. The final result is analysis-ready reads.
 */
@DocumentedFeature
@BetaFeature
public class ReadsPipelineSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public boolean requiresReference() { return true; }


    @Argument(doc = "the known variants", shortName = "knownSites", fullName = "knownSites", optional = false)
    protected List<String> baseRecalibrationKnownVariants;

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    protected String output;

    @Argument(doc = "the join strategy for reference bases and known variants", shortName = "joinStrategy", fullName = "joinStrategy", optional = true)
    private JoinStrategy joinStrategy = JoinStrategy.BROADCAST;

    @Argument(shortName = "DS", fullName ="duplicates_scoring_strategy", doc = "The scoring strategy for choosing the non-duplicate among candidates.")
    public MarkDuplicatesScoringStrategy duplicatesScoringStrategy = MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES;

    /**
     * all the command line arguments for BQSR and its covariates
     */
    @ArgumentCollection(doc = "all the command line arguments for BQSR and its covariates")
    private final RecalibrationArgumentCollection bqsrArgs = new RecalibrationArgumentCollection();

    @ArgumentCollection
    public final HaplotypeCallerSpark.ShardingArgumentCollection shardingArgs = new HaplotypeCallerSpark.ShardingArgumentCollection();

    /**
     * command-line arguments to fine tune the apply BQSR step.
     */
    @ArgumentCollection
    public ApplyBQSRUniqueArgumentCollection applyBqsrArgs = new ApplyBQSRUniqueArgumentCollection();

    @ArgumentCollection
    public HaplotypeCallerArgumentCollection hcArgs = new HaplotypeCallerArgumentCollection();

    @Override
    public SerializableFunction<GATKRead, SimpleInterval> getReferenceWindowFunction() {
        return BaseRecalibrationEngine.BQSR_REFERENCE_WINDOW_FUNCTION;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        if (joinStrategy == JoinStrategy.BROADCAST && ! getReference().isCompatibleWithSparkBroadcast()){
            throw new UserException.Require2BitReferenceForBroadcast();
        }

        //TOOO: should this use getUnfilteredReads? getReads will apply default and command line filters
        final JavaRDD<GATKRead> initialReads = getReads();

        final JavaRDD<GATKRead> markedReadsWithOD = MarkDuplicatesSpark.mark(initialReads, getHeaderForReads(), duplicatesScoringStrategy, new OpticalDuplicateFinder(), getRecommendedNumReducers());
        final JavaRDD<GATKRead> markedReads = MarkDuplicatesSpark.cleanupTemporaryAttributes(markedReadsWithOD);

        // The markedReads have already had the WellformedReadFilter applied to them, which
        // is all the filtering that MarkDupes and ApplyBQSR want. BQSR itself wants additional
        // filtering performed, so we do that here.
        //NOTE: this doesn't honor enabled/disabled commandline filters
        final ReadFilter bqsrReadFilter = ReadFilter.fromList(BaseRecalibrator.getBQSRSpecificReadFilterList(), getHeaderForReads());

        JavaRDD<GATKRead> markedFilteredReadsForBQSR = markedReads.filter(read -> bqsrReadFilter.test(read));

        if (joinStrategy.equals(JoinStrategy.OVERLAPS_PARTITIONER)) {
            // the overlaps partitioner requires that reads are coordinate-sorted
            final SAMFileHeader readsHeader = getHeaderForReads().clone();
            readsHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
            markedFilteredReadsForBQSR = SparkUtils.coordinateSortReads(markedFilteredReadsForBQSR, readsHeader, numReducers);
        }

        VariantsSparkSource variantsSparkSource = new VariantsSparkSource(ctx);
        JavaRDD<GATKVariant> bqsrKnownVariants = variantsSparkSource.getParallelVariants(baseRecalibrationKnownVariants, getIntervals());

        JavaPairRDD<GATKRead, ReadContextData> rddReadContext = AddContextDataToReadSpark.add(ctx, markedFilteredReadsForBQSR, getReference(), bqsrKnownVariants, baseRecalibrationKnownVariants, joinStrategy, getHeaderForReads().getSequenceDictionary(), shardingArgs.readShardSize, shardingArgs.readShardPadding);
        final RecalibrationReport bqsrReport = BaseRecalibratorSparkFn.apply(rddReadContext, getHeaderForReads(), getReferenceSequenceDictionary(), bqsrArgs);

        final Broadcast<RecalibrationReport> reportBroadcast = ctx.broadcast(bqsrReport);
        final JavaRDD<GATKRead> finalReads = ApplyBQSRSparkFn.apply(markedReads, reportBroadcast, getHeaderForReads(), applyBqsrArgs.toApplyBQSRArgumentCollection(bqsrArgs.PRESERVE_QSCORES_LESS_THAN));

        final ReadFilter hcReadFilter = ReadFilter.fromList(HaplotypeCallerEngine.makeStandardHCReadFilters(), getHeaderForReads());
        final JavaRDD<GATKRead> filteredReadsForHC = finalReads.filter(read -> hcReadFilter.test(read));

        // Reads must be coordinate sorted to use the overlaps partitioner
        filteredReadsForHC.persist(StorageLevel.DISK_ONLY()); // without caching, computations are run twice as a side effect of finding partition boundaries for sorting
        final SAMFileHeader readsHeader = getHeaderForReads().clone();
        readsHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        JavaRDD<GATKRead> coordinateSortedReads = SparkUtils.coordinateSortReads(filteredReadsForHC, readsHeader, numReducers);

        final List<SimpleInterval> intervals = hasIntervals() ? getIntervals() : IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        final HaplotypeCallerEngine hcEngine = new HaplotypeCallerEngine(hcArgs, readsHeader, new HaplotypeCallerSpark.ReferenceMultiSourceAdapter(getReference(), getAuthHolder()));
        final JavaRDD<VariantContext> variants = HaplotypeCallerSpark.callVariantsWithHaplotypeCaller(getAuthHolder(), ctx, coordinateSortedReads, readsHeader, getReference(), intervals, hcArgs, shardingArgs);

        if (hcArgs.emitReferenceConfidence == ReferenceConfidenceMode.GVCF) {
            // VariantsSparkSink/Hadoop-BAM VCFOutputFormat do not support writing GVCF, see https://github.com/broadinstitute/gatk/issues/2738
            HaplotypeCallerSpark.writeVariants(output, variants, hcEngine, getReferenceSequenceDictionary(), readsHeader.getSequenceDictionary());
        } else {
            variants.cache(); // without caching, computations are run twice as a side effect of finding partition boundaries for sorting
            try {
                VariantsSparkSink.writeVariants(ctx, output, variants, hcEngine.makeVCFHeader(readsHeader.getSequenceDictionary(), new HashSet<>()));
            } catch (IOException e) {
                throw new UserException.CouldNotCreateOutputFile(output, "writing failed", e);
            }
        }
    }
}
