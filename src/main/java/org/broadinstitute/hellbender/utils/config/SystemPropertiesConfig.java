package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Mutable;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

/**
 * Configuration file for System-level options.
 * All options in this class are intended to be injected into the JRE as System Properties.
 */
@LoadPolicy(LoadType.MERGE)
@Sources({ "${SYSTEM_PROPERTIES_CONFIG_FILE}",                                         // User-defined path
        "file:SystemProperties.config",                                                     // Default path
        "classpath:org/broadinstitute/hellbender/utils/config/SystemProperties.config" })   // Class path
public interface SystemPropertiesConfig extends Mutable, Accessible {

    // =================================================================================
    // General Options:
    // =================================================================================

    @DefaultValue("true")
    boolean GATK_STACKTRACE_ON_USER_EXCEPTION();

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