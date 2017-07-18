package org.broadinstitute.hellbender.tools.walkers.vqsr;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.text.XReadLines;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.lang.Math.abs;

/*
 * Represents a VQSLOD tranche in VQSR for use in scattered VariantRecalibrator runs.
 * (Package-private because it's not usable outside.)
 */
public class VQSLODTranche extends Tranche {
    private static final int CURRENT_VERSION = 6;

    static final Comparator<VQSLODTranche> TRANCHE_ORDER = (tranche1, tranche2) -> Double.compare(tranche1.minVQSLod, tranche2.minVQSLod);

    //TODO: refine these
    public static List<Double> VQSLODoutputs = new ArrayList<>(1000);
    {
        for (double i=10.0; i>5; i-=0.1) {
            VQSLODoutputs.add(i);
        }
        for (double i=5.0; i>-5; i-=0.01) {
            VQSLODoutputs.add(i);
        }
        for (double i=-5.0; i>-10; i-=0.1) {
            VQSLODoutputs.add(i);
        }
    };

    public String getTrancheIndex() {
        return Double.toString(minVQSLod);
    }

    public VQSLODTranche(
            final double minVQSLod,
            final int numKnown,
            final double knownTiTv,
            final int numNovel,
            final double novelTiTv,
            final int accessibleTruthSites,
            final int callsAtTruthSites,
            final VariantRecalibratorArgumentCollection.Mode model,
            final String name) {
        super(name, knownTiTv, numNovel, minVQSLod, model, novelTiTv, accessibleTruthSites, numKnown, callsAtTruthSites);
    }

    @Override
    public String toString() {
        return String.format("Tranche minVQSLod=%.4f known=(%d @ %.4f) novel=(%d @ %.4f) truthSites(%d accessible, %d called), name=%s]",
                minVQSLod, numKnown, knownTiTv, numNovel, novelTiTv, accessibleTruthSites, callsAtTruthSites, name);
    }

    protected static VQSLODTranche trancheOfVariants(final List<VariantDatum> data, final int minI, final VariantRecalibratorArgumentCollection.Mode model ) {
        int numKnown = 0, numNovel = 0, knownTi = 0, knownTv = 0, novelTi = 0, novelTv = 0;

        final double minLod = data.get(minI).lod;
        for ( final VariantDatum datum : data ) {
            if ( datum.lod >= minLod ) {
                if ( datum.isKnown ) {
                    numKnown++;
                    if( datum.isSNP ) {
                        if ( datum.isTransition ) { knownTi++; } else { knownTv++; }
                    }
                } else {
                    numNovel++;
                    if( datum.isSNP ) {
                        if ( datum.isTransition ) { novelTi++; } else { novelTv++; }
                    }
                }
            }
        }

        final double knownTiTv = knownTi / Math.max(1.0 * knownTv, 1.0);
        final double novelTiTv = novelTi / Math.max(1.0 * novelTv, 1.0);

        final int accessibleTruthSites = VariantDatum.countCallsAtTruth(data, Double.NEGATIVE_INFINITY);
        final int nCallsAtTruth = VariantDatum.countCallsAtTruth(data, minLod);

        return new VQSLODTranche(minLod, numKnown, knownTiTv, numNovel, novelTiTv, accessibleTruthSites, nCallsAtTruth, model, DEFAULT_TRANCHE_NAME);
    }

    /**
     * Returns a list of tranches, sorted from most to least specific, read in from file f.
     * @throws IOException if there are problems reading the file.
     */
    public static List<VQSLODTranche> readTranches(final File f) throws IOException{
        String[] header = null;
        List<VQSLODTranche> tranches = new ArrayList<>();

        try (XReadLines xrl = new XReadLines(f) ) {
            for (final String line : xrl) {
                if (line.startsWith(COMMENT_STRING)) {
                    if ( !line.contains("Version"))
                        continue;
                    else {
                        String[] words = line.split("\\s+"); //split on whitespace
                        if (Integer.parseInt(words[3]) != CURRENT_VERSION)
                            throw new UserException.BadInput("The file " + " contains version " + words[3] + "tranches, but VQSLOD tranche parsing requires version " + CURRENT_VERSION);
                        continue;
                    }
                }

                final String[] vals = line.split(VALUE_SEPARATOR);
                if (header == null) {  //reading the header
                    header = vals;
                    if (header.length != EXPECTED_COLUMN_COUNT) {
                        throw new UserException.MalformedFile(f, "Expected " + EXPECTED_COLUMN_COUNT + " elements in header line " + line);
                    }
                } else {
                    if (header.length != vals.length) {
                        throw new UserException.MalformedFile(f, "Line had too few/many fields.  Header = " + header.length + " vals " + vals.length + ". The line was: " + line);
                    }

                    Map<String, String> bindings = new LinkedHashMap<>();
                    for (int i = 0; i < vals.length; i++) {
                        bindings.put(header[i], vals[i]);
                    }
                    tranches.add(new VQSLODTranche(
                            getRequiredDouble(bindings, "minVQSLod"),
                            getOptionalInteger(bindings, "numKnown", -1),
                            getOptionalDouble(bindings, "knownTiTv", -1.0),
                            getRequiredInteger(bindings, "numNovel"),
                            getRequiredDouble(bindings, "novelTiTv"),
                            getOptionalInteger(bindings, "accessibleTruthSites", -1),
                            getOptionalInteger(bindings, "callsAtTruthSites", -1),
                            VariantRecalibratorArgumentCollection.Mode.valueOf(bindings.get("model")),
                            bindings.get("filterName")));
                }
            }
        }

        tranches.sort(TRANCHE_ORDER);
        return tranches;
    }

    public static List<VQSLODTranche> mergeTranches(final Map<Double, List<VQSLODTranche>> scatteredTranches, final List<Double> tsLevels, final VariantRecalibratorArgumentCollection.Mode mode) {
        List<VQSLODTranche> mergedTranches = new ArrayList<>();
        List<VQSLODTranche> gatheredTranches = new ArrayList<>();

        //make a list of merged tranches of the same length
        for (final Double VQSLODlevel : VQSLODoutputs) {
            mergedTranches.add(mergeTranches(scatteredTranches.get(VQSLODlevel),mode));
        }

        //go through the list of merged tranches to select those that match most closely the tsLevels
        //assume tsLevels are sorted and tranches are sorted
        ListIterator<Double> tsIter= tsLevels.listIterator();
        double targetTS = tsIter.next();
        double sensitivityDelta = 1.0; //initialize to 100%
        double prevDelta;
        ListIterator<VQSLODTranche> trancheIter = mergedTranches.listIterator();
        VQSLODTranche currentTranche = trancheIter.next();
        VQSLODTranche prevTranche;

        //match the calculated tranches with the requested truth sensitivity outputs as best as possible
        while (trancheIter.hasNext()) {  //we can only add tranches that we have and we should only add each at most once
            prevDelta = sensitivityDelta;
            prevTranche = currentTranche;
            currentTranche = trancheIter.next();
            sensitivityDelta = abs(targetTS - currentTranche.getTruthSensitivity());
            if (sensitivityDelta > prevDelta) {
                gatheredTranches.add(prevTranche);
                if (tsIter.hasNext()) {
                    targetTS = tsIter.next();
                }
                else {
                    break;
                }
            }
            else if ( !trancheIter.hasNext()) { //if we haven't seen the best match, but we ran out of tranches
                gatheredTranches.add(currentTranche);
            }
        }

        return gatheredTranches;
    }

    public static VQSLODTranche mergeTranches(final List<VQSLODTranche> scatteredTranches, VariantRecalibratorArgumentCollection.Mode mode) {
        double indexVQSLOD = scatteredTranches.get(0).minVQSLod;
        int sumNumKnown = 0;
        double sumKnownTransitions = 0;
        double sumKnownTransversions = 0;
        int sumNumNovel = 0;
        double sumNovelTransitions = 0;
        double sumNovelTransversions = 0;
        int sumAccessibleTruthSites = 0;
        int sumCallsAtTruthSites = 0;

        for (final VQSLODTranche tranche : scatteredTranches) {
            if (tranche.minVQSLod != indexVQSLOD)
                throw new IllegalStateException("Scattered tranches do not contain the same VQSLODs");
            sumNumKnown += tranche.numKnown;
            double trancheKnownTransitions = (tranche.knownTiTv*tranche.numKnown) / (1+tranche.knownTiTv);
            sumKnownTransitions += trancheKnownTransitions;
            sumKnownTransversions += (tranche.numKnown - trancheKnownTransitions);
            sumNumNovel += tranche.numNovel;
            double trancheNovelTransitions = (tranche.novelTiTv*tranche.numNovel) / (1+tranche.novelTiTv);
            sumNovelTransitions += trancheNovelTransitions;
            sumNovelTransversions += (tranche.numNovel - trancheNovelTransitions);
            sumAccessibleTruthSites += tranche.accessibleTruthSites;
            sumCallsAtTruthSites += tranche.callsAtTruthSites;
        }

        return new VQSLODTranche(indexVQSLOD, sumNumKnown, sumKnownTransitions/sumKnownTransversions, sumNumNovel, sumNovelTransitions/sumNovelTransversions, sumAccessibleTruthSites, sumCallsAtTruthSites, mode, "gathered" + indexVQSLOD);
    }
}
