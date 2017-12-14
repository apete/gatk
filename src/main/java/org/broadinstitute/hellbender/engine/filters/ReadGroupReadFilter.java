package org.broadinstitute.hellbender.engine.filters;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.utils.help.HelpConstants;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;

/**
 * Keep only reads from the specified read group. Discards reads lacking an RG tag.
 *
 * <p>Matching is done by case-sensitive exact match.</p>
 */
@DocumentedFeature(groupName= HelpConstants.DOC_CAT_READFILTERS, groupSummary=HelpConstants.DOC_CAT_READFILTERS_SUMMARY, summary = "Keep only reads from the specified read group")
public final class ReadGroupReadFilter extends ReadFilter implements Serializable{
    private static final long serialVersionUID = 1L;
    public static final String KEEP_READ_GROUP_LONG_NAME = "keep-read-group";

    @Argument(fullName = KEEP_READ_GROUP_LONG_NAME, shortName = "keepReadGroup", doc="The name of the read group to keep", optional=false)
    public String readGroup = null;

    @Override
    public boolean test( final GATKRead read ) {
        final String rg = read.getReadGroup();
        return readGroup != null && rg.equals(this.readGroup);
    }
}