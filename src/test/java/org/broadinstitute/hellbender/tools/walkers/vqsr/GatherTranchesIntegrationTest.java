package org.broadinstitute.hellbender.tools.walkers.vqsr;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by gauthier on 7/18/17.
 */
public class GatherTranchesIntegrationTest extends CommandLineProgramTest {

    private static final String testDir = BaseTest.publicTestDir + "/large/VQSR/expected/";

    @Test
    public void testCombine2Shards() throws Exception {
        final File recal1 = new File(testDir + "snpTranches.scattered.txt");
        final File recal2 = new File(testDir + "snpTranches.scattered.2.txt");

        final File recal_original = new File(testDir + "snpTranches.gathered.txt");

        final ArgumentsBuilder args = new ArgumentsBuilder();
        args.add("--input");
        args.add(recal1.getAbsolutePath());
        args.add("--input");
        args.add(recal2.getAbsolutePath());

        final File outFile = BaseTest.createTempFile("gatheredTranches", ".txt");
        args.addOutput(outFile);
        final Object res = this.runCommandLine(args.getArgsArray());
        Assert.assertEquals(res, 0);
        IntegrationTestSpec.assertEqualTextFiles(outFile, recal_original);
    }


}