package org.broadinstitute.hellbender.tools.copynumber.legacy;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.ExomeStandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.*;

/**
 * Integration test for {@link DenoiseReadCounts}.
 *
 * @author Samuel Lee &lt;slee@broadinstitute.org&gt;
 */
public class DenoiseReadCountsIntegrationTest extends CommandLineProgramTest {
    private static final String TEST_SUB_DIR = publicTestDir + "org/broadinstitute/hellbender/tools/copynumber/allelic";
    private static final File NORMAL_READ_COUNT_FILE = new File("/home/slee/working/ipython/wes.tsv");

    @Test
    public void testWES() {
        final String[] arguments = {
                "-" + StandardArgumentDefinitions.INPUT_SHORT_NAME, "/home/slee/working/ipython/wes_case.tsv",
                "-" + ExomeStandardArgumentDefinitions.PON_FILE_SHORT_NAME, "/home/slee/working/ipython/wes.no-gc.pon",
                "-" + ExomeStandardArgumentDefinitions.PRE_TANGENT_NORMALIZED_COUNTS_FILE_SHORT_NAME, "/home/slee/working/ipython/wes.no-gc.ptn.tsv",
                "-" + ExomeStandardArgumentDefinitions.TANGENT_NORMALIZED_COUNTS_FILE_SHORT_NAME, "/home/slee/working/ipython/wes.no-gc.tn.tsv",
//                "-" + DenoiseReadCounts.NUMBER_OF_EIGENSAMPLES_SHORT_NAME, "10",
                "--" + StandardArgumentDefinitions.VERBOSITY_NAME, "INFO"
        };
        runCommandLine(arguments);
    }
}