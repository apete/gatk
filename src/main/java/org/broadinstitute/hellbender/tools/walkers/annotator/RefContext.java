package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by tsato on 7/20/17.
 */
public class RefContext extends InfoFieldAnnotation {
    @Override
    public List<String> getKeyNames(){
        return(Collections.singletonList(GATKVCFConstants.REFERENCE_CONTEXT_KEY));
    }

    @Override
    public Map<String, Object> annotate(final ReferenceContext ref,
                                        final VariantContext vc,
                                        final ReadLikelihoods<Allele> likelihoods){
        Utils.nonNull(ref);
        Utils.nonNull(vc);
        Utils.nonNull(likelihoods);

        final int k = 3;
        final byte[] referenceBases = ref.getBasesInInterval(vc.getStart() - k/2, vc.getStart() + k/2 + 1);
        return Collections.singletonMap(getKeyNames().get(0), new String(referenceBases));
    }
}
