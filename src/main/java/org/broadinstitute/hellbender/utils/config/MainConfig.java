package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Mutable;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

import java.io.PrintStream;
import java.util.List;

/**
 * Configuration file for GATK options.
 */
@LoadPolicy(LoadType.MERGE)
@Sources({"file:Main.config",                                                   // Default path
        "classpath:org/broadinstitute/hellbender/utils/config/Main.config" })   // Class path
public interface MainConfig extends Mutable, Accessible {

    // =================================================================================
    // General Options:
    // =================================================================================

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
}