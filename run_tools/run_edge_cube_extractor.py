# -*- coding: utf-8 -*-
"""Run Script for the EDGE CUBE extractor."""
# ## IMPORTS ## #
# Standard imports
import os
import sys
import shutil

# Third party imports
from pathlib import Path
from tqdm import tqdm


# Local imports
sys.path.append(".")
sys.path.append("..")
from normits_demand import logging as nd_log
from normits_demand.tools import edge_cube_extractor
from normits_demand.utils import file_ops
from normits_demand.matrices.cube_mat_converter import CUBEMatConverter

# TODO (MI) Setup user inputs using BaseConfig

# ## USER INPUTS ## #
# cube catalogue setup
CUBE_EXE = Path(r"C:\Program Files\Citilabs\CubeVoyager\VOYAGER.EXE")
CUBE_CAT_PATH = Path(r"C:\GitHub\NorTMS")
CAT_RUN_DIR = "Scenarios/Base"
CUBE_RUN_ID = "OGK_2018"
NETWORK_VERSION = 63
DEMAND_VERSION = 10
DIMENSION_VERSION = 3
# process parts
# EXPORT_MATRICES to export NoRMS base matrices into CSVs
# EXPORT_TLC to NoRMS <-> MOIRA TLCs Lookup
EXPORT_MATRICES = True
EXPORT_TLC = True

# Input files
TLC_OVERWRITE_PATH = Path(r"U:\00_Inputs\GATInputs\TLC_Overwrite_MOIRA.csv")
TICKET_SPLITS_PATH = Path(r"U:\00_Inputs\GATInputs\TicketTypeSplits.csv")

# Output location
OUT_PATH = Path(r"C:\Work\NoRMS\temp\normits")

# ## CONSTANTS ## #
# logger
LOG_FILE = "Export_BaseMatrices_Logfile.Log"
LOG = nd_log.get_logger(f"{nd_log.get_package_logger_name()}.run_edge_cube_extractor")

# Derived from inputs
CUBE_CAT_RUN_PATH = CUBE_CAT_PATH / CAT_RUN_DIR / CUBE_RUN_ID

# ## CLASSES ## #


# ## FUNCTIONS ## #
def run_extractor():
    """Process Fixed objects."""
    # create splits directory
    os.makedirs(OUT_PATH / "split_dir")
    shutil.copy2(
        TICKET_SPLITS_PATH,
        OUT_PATH / "split_dir/TicketTypeSplits.csv",
    )
    if EXPORT_TLC:
        # produce TLC lookup
        file_ops.check_file_exists(TLC_OVERWRITE_PATH)
        tlc_overwrite = file_ops.read_df(TLC_OVERWRITE_PATH)
        stns_tlc = edge_cube_extractor.stnzone_2_stn_tlc(
            CUBE_CAT_RUN_PATH / f"Inputs/Network/v{NETWORK_VERSION}/Station_Connectors.csv",
            CUBE_CAT_RUN_PATH / f"Inputs/Network/v{NETWORK_VERSION}/TfN_Rail_Nodes.csv",
            CUBE_CAT_RUN_PATH
            / f"Inputs/Network/v{NETWORK_VERSION}/External_Station_Nodes.csv",
            tlc_overwrite,
        )

        # write TLC Lookup
        file_ops.write_df(stns_tlc, OUT_PATH / "TLCs.csv", index=False)

        LOG.info("TLCs overwrite file exported")

    if EXPORT_MATRICES:
        # time periods
        periods = ["AM", "IP", "PM", "OP"]

        # copy Cube files
        for period in periods:
            # read distance matrix
            file_ops.check_file_exists(
                CUBE_CAT_RUN_PATH
                / f"Outputs/InitialAssignment/{period}_Stn2Stn_AVG_Skims.MAT",
            )
            shutil.copy2(
                CUBE_CAT_RUN_PATH
                / f"Outputs/InitialAssignment/{period}_Stn2Stn_AVG_Skims.MAT",
                OUT_PATH / f"{period}_Stn2Stn_Costs.MAT",
            )

            # read iRSj props
            file_ops.check_file_exists(
                CUBE_CAT_RUN_PATH / f"Outputs/InitialAssignment/{period}_ixRSyj.PRQ",
            )
            shutil.copy2(
                CUBE_CAT_RUN_PATH / f"Outputs/InitialAssignment/{period}_ixRSyj.PRQ",
                OUT_PATH / f"{period}_ixRSyj.PRQ",
            )

            LOG.info(
                "Distance and Probability matrices for period %s has been copied",
                period,
            )

        # PT Demand to time periods F/T
        edge_cube_extractor.pt_demand_from_to(
            CUBE_EXE,
            CUBE_CAT_PATH,
            CUBE_CAT_RUN_PATH,
            DEMAND_VERSION,
            DIMENSION_VERSION,
            OUT_PATH,
        )
        LOG.info("NoRMS matrices converted to OMX successfully")

        # export to OMX
        for period in tqdm(periods, desc="Time Periods Loop ", unit="Period"):
            c_m = CUBEMatConverter(CUBE_EXE)
            c_m.mat_2_omx(OUT_PATH / f"PT_{period}.MAT", OUT_PATH, f"PT_{period}")
            # delete .MAT files
            os.remove(f"{OUT_PATH}/PT_{period}.MAT")
            edge_cube_extractor.export_mat_2_csv_via_omx(
                CUBE_EXE,
                OUT_PATH / f"{period}_Stn2Stn_Costs.MAT",
                OUT_PATH / "split_dir",
                f"{period}_Stn2Stn_Costs",
                ["Dist_R"],
            )
            # rename
            os.rename(
                OUT_PATH / f"split_dir/{period}_Stn2Stn_Costs_Dist_R.csv",
                OUT_PATH / f"split_dir/{period}_stn2stn_costs.csv",
            )
            # export costs to csv

            LOG.info(f"{period} NoRMS matrices exported to CSVs")

        LOG.info("#" * 80)
        LOG.info("Process Finished Successfully")
        LOG.info("#" * 80)


def main():
    """Main Function."""
    # Set up a logger to capture all log outputs and warnings
    nd_log.get_logger(
        logger_name=nd_log.get_package_logger_name(),
        log_file_path=os.path.join(OUT_PATH, LOG_FILE),
        instantiate_msg="Export NoRMS Base Demand",
        log_version=True,
    )
    nd_log.capture_warnings(file_handler_args=dict(log_file=OUT_PATH / LOG_FILE))
    run_extractor()


if __name__ == "__main__":
    main()
