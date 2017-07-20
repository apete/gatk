package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by tsato on 7/20/17.
 */
public class OrientationArtifact extends GenotypeAnnotation {
    @Override
    public List<String> getKeyNames() {
        return Collections.singletonList(GATKVCFConstants.REFERENCE_CONTEXT_KEY);
    }

    @Override
    public List<VCFFormatHeaderLine> getDescriptions() {
        return Collections.singletonList(VCFStandardHeaderLines.getFormatLine(GATKVCFConstants.REFERENCE_CONTEXT_KEY));
    }

    @Override
    public void annotate( final ReferenceContext ref,
                          final VariantContext vc,
                          final Genotype g,
                          final GenotypeBuilder gb,
                          final ReadLikelihoods<Allele> likelihoods ) {
        Utils.nonNull(vc);
        Utils.nonNull(g);
        Utils.nonNull(gb);

        final int k = 3;
        SimpleInterval oldWindow = ref.getWindow();
        ref.setWindow(k/2, k/2);
        final byte[] refContext = ref.getBases();




    }



}
