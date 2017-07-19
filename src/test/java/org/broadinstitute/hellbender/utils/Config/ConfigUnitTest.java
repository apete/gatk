package org.broadinstitute.hellbender.utils.Config;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.config.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Unit test for GATK configuration file handling.
 *
 * Created by jonn on 7/19/17.
 */
public class ConfigUnitTest {

    // ================================================================================
    // Data Providers:
    // ================================================================================

    @DataProvider
    Object[][] createArgsAndConfigFileOptions() {
        return new Object[][] {
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","--zonfigurati","DUMMY_FILE","END"})),
                    "--config",
                    null,
                    new ArrayList<>(Arrays.asList(new String[] {"main","--zonfigurati","DUMMY_FILE","END"})),
                },
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","--config","DUMMY_FILE","END"})),
                    "--config",
                    "DUMMY_FILE",
                    new ArrayList<>(Arrays.asList(new String[] {"main","END"})),
                },
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","END","--config","DUMMY_FILE"})),
                    "--config",
                    "DUMMY_FILE",
                    new ArrayList<>(Arrays.asList(new String[] {"main","END"})),
                },
        };
    }

    @DataProvider
    Object[][] createArgsAndConfigFileOptionsBadInput() {
        return new Object[][] {
                {
                        new ArrayList<>(Arrays.asList(new String[] {"main", "testArg", "--config"})),
                        "--config",
                },
                {
                        new ArrayList<>(Arrays.asList(new String[] {"main","--config"})),
                        "--config",
                },
                {
                        new ArrayList<>(Arrays.asList(new String[] {"--config"})),
                        "--config",
                },
        };
    }

    // ================================================================================
    // Tests:
    // ================================================================================

    @Test(dataProvider = "createArgsAndConfigFileOptionsBadInput",
            expectedExceptions = UserException.BadInput.class)
    void testGetConfigFilenameFromArgsBadInput( final ArrayList<String> args,
                                        final String configFileOption) {

        ConfigUtils.getConfigFilenameFromArgs(args, configFileOption);
    }

    @Test(dataProvider= "createArgsAndConfigFileOptions")
    void testGetConfigFilenameFromArgs( final ArrayList<String> args,
                                        final String configFileOption,
                                        final String expectedFilename,
                                        final ArrayList<String> expectedRemainingArgs) {

        String outFileName = ConfigUtils.getConfigFilenameFromArgs(args, configFileOption);

        Assert.assertEquals(expectedFilename, outFileName);
        Assert.assertEquals(expectedRemainingArgs, args);
    }

}
