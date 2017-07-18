package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.*;

import java.util.List;

/**
 * Configuration file for top-level options.
 */
@LoadPolicy(LoadType.MERGE)
@Sources({ "file:Main.config",
        "classpath:org/broadinstitute/hellbender/utils/config/Main.config" })
public interface MainConfig extends Config {

    // =================================================================================
    // General Options:
    // =================================================================================

    @DefaultValue("true")
    boolean GATK_STACKTRACE_ON_USER_EXCEPTION();

    @DefaultValue(
            "htsjdk.variant,htsjdk.tribble,org.broadinstitute.hellbender.utils.codecs")
    List<String> codec_packages();

    // =================================================================================
    // GATKTool Options:
    // =================================================================================

    @DefaultValue("40")
    int cloudPrefetchBuffer();

    @DefaultValue("-1")
    int cloudIndexPrefetchBuffer();

    @DefaultValue("true")
    boolean createOutputBamIndex();

    // =================================================================================
    // SAMJDK Options:
    // =================================================================================

    @Key("samjdk.use_async_io_read_samtools")
    @DefaultValue("false")
    boolean samjdk_use_async_io_read_samtools();

    @Key("samjdk.use_async_io_write_samtools")
    @DefaultValue("true")
    boolean samjdk_use_async_io_write_samtools();

    @Key("samjdk.use_async_io_write_tribble")
    @DefaultValue("false")
    boolean samjdk_use_async_io_write_tribble();

    @Key("samjdk.compression_level")
    @DefaultValue("1")
    int samjdk_compression_level();

    // =================================================================================
    // Other Options:
    // =================================================================================

    @Key("snappy.disable")
    @DefaultValue("true")
    boolean snappy_disable();

}