# -*- coding: utf-8 -*-
"""
Created on: 07/12/2021
Updated on:

Original author: Chris Storey
Last update made by: Ben Taylor
Other updates made by:

File purpose:

"""
# Built-Ins
import os
import sys

import itertools

# Third Party

# Local Imports
sys.path.append("..")
# pylint: disable=import-error,wrong-import-position
from normits_demand import core as nd_core
from normits_demand.tools import trip_length_distributions as tlds

# pylint: enable=import-error,wrong-import-position

# GLOBAL
TLB_FOLDER = "I:/NTS/outputs/tld"
TLB_VERSION = "nts_tld_data_v3.1.csv"
OUTPUT_FOLDER = r"I:\NorMITs Demand\import\trip_length_distributions\tld_tool_outputs"
TLD_HOME = r"I:\NorMITs Demand\import\trip_length_distributions\config"

BAND_DIR = os.path.join(TLD_HOME, "bands")
SEGMENTATION_DIR = os.path.join(TLD_HOME, "segmentations")
COPY_DEFINITIONS_DIR = os.path.join(SEGMENTATION_DIR, "copy_defs")


def run_all_combinations():
    """Runs every combination of inputs through the TLD builder"""
    # Get a list of all available options
    band_list = os.listdir(BAND_DIR)
    band_list = [x for x in band_list if ".csv" in x]

    seg_list = os.listdir(SEGMENTATION_DIR)
    seg_list = [x for x in seg_list if ".csv" in x]

    extract = tlds.TripLengthDistributionBuilder(
        tlb_folder=TLB_FOLDER,
        tlb_version=TLB_VERSION,
        bands_definition_dir=BAND_DIR,
        segment_definition_dir=SEGMENTATION_DIR,
        segment_copy_definition_dir=COPY_DEFINITIONS_DIR,
        output_folder=OUTPUT_FOLDER,
    )

    for area, bands, seg in itertools.product(list(tlds.GeoArea), band_list, seg_list):
        # Built list of unchanging kwargs
        kwargs = {
            "geo_area": area,
            "sample_period": tlds.SampleTimePeriods.FULL_WEEK,
            "cost_units": nd_core.CostUnits.KM,
            "bands_name": bands,
            "segmentation_name": seg,
            "sample_threshold": 10,
            "verbose": False,
        }

        extract.tld_generator(trip_filter_type=tlds.TripFilter.TRIP_OD, **kwargs)

        # Include ie movements filter too if not GB
        if area != tlds.GeoArea.GB:
            extract.tld_generator(trip_filter_type=tlds.TripFilter.TRIP_O, **kwargs)


def run_test():
    """Runs a test set of inputs through the TLD builder"""
    # Get a list of all available options
    band_list = os.listdir(BAND_DIR)
    band_list = [x for x in band_list if ".csv" in x]

    seg_list = os.listdir(SEGMENTATION_DIR)
    seg_list = [x for x in seg_list if ".csv" in x]

    extract = tlds.TripLengthDistributionBuilder(
        tlb_folder=TLB_FOLDER,
        tlb_version=TLB_VERSION,
        bands_definition_dir=BAND_DIR,
        segment_definition_dir=SEGMENTATION_DIR,
        segment_copy_definition_dir=COPY_DEFINITIONS_DIR,
        output_folder=OUTPUT_FOLDER,
    )

    kwargs = {
        "geo_area": tlds.GeoArea.NORTH,
        "sample_period": tlds.SampleTimePeriods.FULL_WEEK,
        "cost_units": nd_core.CostUnits.KM,
        "bands_name": band_list[0],
        "segmentation_name": seg_list[0],
        "sample_threshold": 10,
        "verbose": False,
    }

    # North
    extract.tld_generator(trip_filter_type=tlds.TripFilter.TRIP_OD, **kwargs)

    # North inc_ie
    extract.tld_generator(trip_filter_type=tlds.TripFilter.TRIP_O, **kwargs)


def build_new_dimo_tlds():
    """Build a new version of all the TLDs needed for the distribution model"""
    # This has light and heavy rail split out for tram TLDs
    tlb_version = "nts_tld_data_v3.1.csv"

    # Init
    extractor = tlds.TripLengthDistributionBuilder(
        tlb_folder=TLB_FOLDER,
        tlb_version=tlb_version,
        bands_definition_dir=BAND_DIR,
        segment_definition_dir=SEGMENTATION_DIR,
        segment_copy_definition_dir=COPY_DEFINITIONS_DIR,
        output_folder=OUTPUT_FOLDER,
    )

    # Define consistent kwargs
    path_kwargs = {
        "trip_filter_type": tlds.TripFilter.TRIP_OD,
        "sample_period": tlds.SampleTimePeriods.FULL_WEEK,
        "cost_units": nd_core.CostUnits.KM,
    }
    generate_kwargs = path_kwargs.copy()
    generate_kwargs.update(
        {
            "sample_threshold": 10,
            "verbose": False,
        }
    )

    for geo_area in [tlds.GeoArea.GB, tlds.GeoArea.NORTH_AND_MIDS]:
        # ## GENERATE HIGHWAY ## #
        hway_bands = "dm_highway_bands"
        hway_kwargs = generate_kwargs.copy()
        hway_kwargs.update({"geo_area": geo_area, "bands_name": hway_bands})

        # HB TLDs
        extractor.tld_generator(segmentation_name="hb_p_m", **hway_kwargs)

        # NHB TLDs - car has lots of data, can be done at time periods
        extractor.tld_generator(segmentation_name="nhb_p_m_tp_car", **hway_kwargs)

        # NHB TLDs - other modes need generating at 24hr and duplicating
        extractor.tld_generator(segmentation_name="nhb_p_m", **hway_kwargs)
        extractor.copy_across_tps(
            geo_area=geo_area,
            bands_name=hway_bands,
            segmentation_name="nhb_p_m",
            **path_kwargs,
        )

        # ## GENERATE RAIL ## #
        rail_bands = "dm_north_rail_bands"
        if geo_area == tlds.GeoArea.GB:
            rail_bands = "dm_gb_rail_bands"

        rail_kwargs = generate_kwargs.copy()
        rail_kwargs.update({"geo_area": geo_area, "bands_name": rail_bands})

        # HB TLDs
        extractor.tld_generator(segmentation_name="hb_p_m_ca_m6", **rail_kwargs)

        # NHB TLDs - other modes need generating at 24hr and duplicating
        extractor.tld_generator(segmentation_name="nhb_p_m_ca_m6", **rail_kwargs)
        extractor.copy_across_tps(
            geo_area=geo_area,
            bands_name=rail_bands,
            segmentation_name="nhb_p_m_ca_m6",
            **path_kwargs,
        )


def build_new_traveller_segment_tlds():
    """Build a new version of all the TLDs needed for the traveller segments"""
    # NOTE: USING 3.0 to keep light rail in with heavy
    tlb_version = "nts_tld_data_v3.0.csv"

    # Init
    extract = tlds.TripLengthDistributionBuilder(
        tlb_folder=TLB_FOLDER,
        tlb_version=tlb_version,
        bands_definition_dir=BAND_DIR,
        segment_definition_dir=SEGMENTATION_DIR,
        segment_copy_definition_dir=COPY_DEFINITIONS_DIR,
        output_folder=OUTPUT_FOLDER,
    )

    path_kwargs = {
        "geo_area": tlds.GeoArea.GB,
        "trip_filter_type": tlds.TripFilter.TRIP_OD,
        "sample_period": tlds.SampleTimePeriods.FULL_WEEK,
        "cost_units": nd_core.CostUnits.KM,
    }
    generate_kwargs = path_kwargs.copy()
    generate_kwargs.update(
        {
            "sample_threshold": 10,
            "verbose": False,
        }
    )

    # ## GENERATE RAIL TLDS ## #

    # Generate with CA combined and then split out
    # Generate with HB and NHB combined and then split out
    extract.tld_generator(
        bands_name="dia_gb_rail_bands",
        segmentation_name="uc_m_seg_m6",
        **generate_kwargs,
    )

    # Copy back out!
    extract.copy_tlds(
        copy_definition_name="traveller_segment_rail",
        bands_name="dia_gb_rail_bands",
        segmentation_name="uc_m_seg_m6",
        **path_kwargs,
    )

    # A full traveller segment run needs:
    # ## run at GB, trip_OD ## #
    #
    #   dm_highway_bands
    #       hb_business
    #       hb_commute
    #       hb_other
    #       nhb_business
    #       nhb_other
    #


if __name__ == "__main__":
    # run_test()

    # run_all_combinations()
    build_new_dimo_tlds()
    # build_new_traveller_segment_tlds()
