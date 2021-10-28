# -*- coding: utf-8 -*-
"""
Created on: Wednesday Sept 22nd 2021

Original author: Nirmal Kumar

File purpose:
Tram inclusion on NoTEM outputs
"""
# Allow class self type hinting
from __future__ import annotations

# Builtins
import os

from typing import Dict, Tuple
from typing import List

# Third party imports
import pandas as pd
import numpy as np

# local imports
import normits_demand as nd

from normits_demand.utils import timing
from normits_demand.utils import file_ops
from normits_demand.utils import general as du
from normits_demand.utils import math_utils
from normits_demand.utils import pandas_utils as pd_utils

from normits_demand.pathing import NoTEMExportPaths


class Tram(NoTEMExportPaths):
    """
    Tram Inclusion on NoTEM outputs
    """
    # Constants
    _sort_msoa = ['msoa_zone_id', 'p', 'ca', 'm']
    _sort_north = ['p', 'ca', 'm']
    _join_cols = ['msoa_zone_id', 'p', 'ca']
    base_train = pd.DataFrame()
    _log_fname = "tram_log.log"
    _notem_cols = ['msoa_zone_id', 'p', 'ca', 'm', 'val']

    _zoning_system_col = 'msoa_zone_id'
    _tram_segment_cols = ['p', 'm', 'ca']
    _val_col = 'val'

    # Define wanted columns
    _target_col_dtypes = {
        'tram': {
            'msoa_zone_id': str,
            'p': int,
            'ca': int,
            'm': int,
            'trips': float
        },
        'notem': {
            'msoa_zone_id': str,
            'p': int,
            'ca': int,
            'm': int,
            'val': float
        },
        'notem_north': {
            'ie_sector_zone_id': int,
            'p': int,
            'ca': int,
            'm': int,
            'val': float
        }
    }

    def __init__(self,
                 years: List[int],
                 scenario: str,
                 iteration_name: str,
                 import_home: nd.PathLike,
                 export_home: nd.PathLike,
                 ):
        """
        Assigns the attributes needed for tram inclusion model.

        Parameters
        ----------
        years:
            List of years to run tram inclusion for. Will assume that the smallest
            year is the base year.

        iteration_name:
            The name of this iteration of the NoTEM models. Will have 'iter'
            prepended to create the folder name. e.g. if iteration_name was
            set to '3i' the iteration folder would be called 'iter3i'.

        scenario:
            The name of the scenario to run for.

        import_home:
            The home location where all the import files are located.

        export_home:
            The home where all the export paths should be built from. See
            nd.pathing.NoTEMExportPaths for more info on how these paths
            will be built.
        """
        # Validate inputs
        file_ops.check_path_exists(import_home)

        # Assign
        self.years = years
        self.scenario = scenario
        self.import_home = import_home

        # Generate the export paths
        super().__init__(
            export_home=export_home,
            path_years=self.years,
            scenario=scenario,
            iteration_name=iteration_name,
        )

        # Create a logger
        logger_name = "%s.%s" % (nd.get_package_logger_name(), self.__class__.__name__)
        log_file_path = os.path.join(self.export_home, self._log_fname)
        self._logger = nd.get_logger(
            logger_name=logger_name,
            log_file_path=log_file_path,
            instantiate_msg="Initialised new Tram Logger",
        )

    def run_tram(self,
                 verbose: bool = True,
                 ) -> None:
        """
        Generates the inputs required for tram inclusion run.

        Runs the tram inclusion for each trip end.

        Parameters
        ----------
        verbose:
            Whether to print progress updates to the terminal while running
            or not.

        Returns
        -------
        None
        """

        start_time = timing.current_milli_time()
        self._logger.info("Starting a new run of the Tram Model")

        # Run the models
        self._generate_hb_production(verbose)
        self._generate_hb_attraction(verbose)
        self._generate_nhb_production(verbose)
        self._generate_nhb_attraction(verbose)

        end_time = timing.current_milli_time()
        time_taken = timing.time_taken(start_time, end_time)
        self._logger.info("Tram Model run complete! Took %s" % time_taken)

    def _generate_hb_production(self, verbose: bool) -> None:
        """
        Runs tram inclusion for home based Production trip end models
        """
        self._logger.info("Generating HB Production imports")
        tram_data = os.path.join(self.import_home, "tram_hb_productions_v1.0.csv")

        export_paths = self.hb_production.export_paths
        hb_production_paths = {y: export_paths.notem_segmented[y] for y in self.years}

        # Runs the home based Production model
        self._logger.info("Adding Tram into HB Productions")
        self._tram_inclusion(
            tram_data=tram_data,
            dvec_imports=hb_production_paths,
            export_home=self.hb_production.export_paths.home,
            verbose=verbose,
        )

    def _generate_hb_attraction(self, verbose: bool) -> None:
        """
        Runs tram inclusion for home based Attraction trip end model
        """
        self._logger.info("Generating HB Attraction imports")
        tram_data = os.path.join(self.import_home, "tram_hb_attractions_v1.0.csv")

        export_paths = self.hb_attraction.export_paths
        hb_attraction_paths = {y: export_paths.notem_segmented[y] for y in self.years}

        self._logger.info("Adding Tram into HB Attractions")
        self._tram_inclusion(
            tram_data=tram_data,
            dvec_imports=hb_attraction_paths,
            export_home=self.hb_attraction.export_paths.home,
            verbose=verbose,
        )

    def _generate_nhb_production(self, verbose: bool) -> None:
        """
        Runs tram inclusion for non-home based Production trip end model
        """
        self._logger.info("Generating NHB Production imports")
        tram_data = os.path.join(self.import_home, "tram_nhb_productions_v1.0.csv")

        export_paths = self.nhb_production.export_paths
        nhb_production_paths = {y: export_paths.notem_segmented[y] for y in self.years}

        self._logger.info("Adding Tram into NHB Productions")
        self._tram_inclusion(
            tram_data=tram_data,
            dvec_imports=nhb_production_paths,
            export_home=self.nhb_production.export_paths.home,
            verbose=verbose,
        )

    def _generate_nhb_attraction(self, verbose: bool) -> None:
        """
        Runs tram inclusion for non home based Attraction trip end models.
        """
        self._logger.info("Generating NHB Production imports")
        tram_data = os.path.join(self.import_home, "tram_hb_attractions_v1.0.csv")

        export_paths = self.nhb_attraction.export_paths
        nhb_attraction_paths = {y: export_paths.notem_segmented[y] for y in self.years}

        self._logger.info("Adding Tram into NHB Attractions")
        self._tram_inclusion(
            tram_data=tram_data,
            dvec_imports=nhb_attraction_paths,
            export_home=self.nhb_attraction.export_paths.home,
            verbose=verbose,
        )

    def _tram_inclusion(self,
                        tram_data: nd.PathLike,
                        dvec_imports: Dict[int, nd.PathLike],
                        export_home: nd.PathLike,
                        verbose: bool = True,
                        ) -> None:
        """
        Runs the tram inclusion for the notem trip end output.

        Completes the following steps for each year:
            - Reads in the base year tram data given.
            - Reads in the notem segmented trip end output compressed pickle
              given.
            - Aggregates above to the required segmentation.
            - Runs the tram infill for the 275 internal msoa zones with tram data.
            - Calculates the future growth of tram based on rail growth and applies it to tram.
            - Runs the tram infill for the entire north.
            - Runs the tram infill for the remaining internal non tram msoa zones
            - Combines the tram infill with the external msoa zone data and
              converts them to Dvector.
            - Disaggregate the Dvector to the notem segmentation.

        Parameters
        ----------
        tram_data:
            The path to the base year tram data.

        dvec_imports:
            Dictionary of {year: notem_segmented_trip_end data} pairs.

        export_home:
            The path where the export file would be saved.

        verbose:
            If set to True, it will print out progress updates while running.

        Returns
        -------
        None

        """
        notem_output = dvec_imports

        # Check that the paths we need exist!
        file_ops.check_file_exists(tram_data)
        [file_ops.check_file_exists(x) for x in notem_output.values()]
        file_ops.check_path_exists(export_home)

        # TODO(BT, NK): Pass this in as an argument
        tram_competitors = [nd.Mode.CAR, nd.Mode.BUS, nd.Mode.TRAIN]

        # Assign
        self.tram_data = tram_data
        self.notem_output = notem_output
        self.export_home = export_home
        self.years = list(self.notem_output.keys())
        self.base_year = min(self.years)

        # Initialise timing
        # TODO: Properly integrate logging
        start_time = timing.current_milli_time()

        # Run tram inclusion for each given year
        for year in self.years:
            trip_end_start = timing.current_milli_time()

            # Read tram and notem output data
            tram_data, notem_tram_seg = self._read_tram_and_notem_data(year)

            # Runs tram infill for 275 internal tram msoa zones
            tram_msoa, notem_msoa_wo_infill = self._tram_infill_msoa(
                notem_tram_seg=notem_tram_seg,
                tram_data=tram_data,
                year=year,
                tram_competitors=tram_competitors,
                verbose=verbose,
            )

            # Runs tram infill for entire north
            north_msoa, north_wo_infill = self._tram_infill_north(
                notem_tram_seg=notem_tram_seg,
                north_tram_data=tram_data,
                tram_competitors=tram_competitors,
                verbose=verbose,
            )

            # Runs tram infill for internal non tram msoa zones (internal msoa zones - 275 internal tram zones)
            non_tram_msoa, north_adj_factor = self._non_tram_infill(
                north_wo_infill=north_wo_infill,
                north_w_infill=north_msoa,
                tram_zones=tram_data['msoa_zone_id'].unique().tolist(),
                dvec_tram_seg=notem_tram_seg,
                tram_competitors=tram_competitors,
                verbose=verbose,
            )

            # TODO(BT): Output report of northern adj factors - north_adj_factor

            # ## STICK THINGS BACK TOGETHER ## #
            df_list = [non_tram_msoa, tram_msoa]
            full_tram_infill = pd.concat(df_list, ignore_index=True)

            # Compare full thing to notem and make sure we haven't dropped anything
            expected_total = notem_tram_seg.sum()
            final_total = full_tram_infill[self._val_col].values.sum()
            if not math_utils.is_almost_equal(expected_total, final_total, rel_tol=0.01):
                raise ValueError(
                    "Some demand seems to have gone missing while infilling tram!\n"
                    "Starting demand: %s\n"
                    "Ending demand: %s"
                    % (expected_total, final_total)
                )

            print("SUCCESSFULLY INFILLED TRAM!")

            print(full_tram_infill)
            exit()

            # Combines the tram infill and converts them to Dvec
            notem_dvec = self._combine_trips(external_notem, tram_msoa, non_tram_msoa, year, verbose)

            # Bring the above Dvec back to original segmentation
            notem_output_dvec = nd.read_pickle(self.notem_output[year])

            # TODO: Need to figure out a way to bring back notem segmentation
            #  notem_dvec = notem_dvec.split_segmentation_like(notem_output_dvec)
            # TODO: export location need to be sorted
            notem_dvec.to_pickle(os.path.join(self.export_home, "tram_included_dvec.pkl"))
            # Print timing for each trip end model
            trip_end_end = timing.current_milli_time()
            time_taken = timing.time_taken(trip_end_start, trip_end_end)
            du.print_w_toggle(
                "Tram inclusion for year %d took: %s\n"
                % (year, time_taken),
                verbose=verbose
            )
        # End timing
        end_time = timing.current_milli_time()
        time_taken = timing.time_taken(start_time, end_time)
        du.print_w_toggle(
            "Tram inclusion took: %s\n"
            "Finished at: %s" % (time_taken, end_time),
            verbose=verbose
        )

    def _read_tram_and_notem_data(self,
                                  year: int,
                                  verbose: bool = False,
                                  ) -> Tuple[pd.DataFrame, nd.DVector]:
        """
        Reads in the tram and notem data.

        Parameters
        ----------
        year:
            The year for which the data needs to be imported.

        verbose:
            If set to True, it will print out progress updates while
            running.

        Returns
        -------
        tram_data:
            Returns the tram data as dataframe.

        notem_tram_seg:
            Returns the notem output dvector in tram segmentation.
        """
        # Init
        du.print_w_toggle("Loading the tram data...", verbose=verbose)
        tram_target_cols = self._target_col_dtypes['tram']

        # Reads the tram data
        tram_data = file_ops.read_df(path=self.tram_data, find_similar=True)
        tram_data['m'] = 7
        tram_data = pd_utils.reindex_cols(tram_data, tram_target_cols)

        # Make sure the input data is in the correct data types
        for col, dt in self._target_col_dtypes['tram'].items():
            tram_data[col] = tram_data[col].astype(dt)

        tram_data.rename(columns={'trips': self._val_col}, inplace=True)

        # Reads the corresponding notem output
        du.print_w_toggle("Loading the notem output data...", verbose=verbose)
        notem_output_dvec = nd.read_pickle(self.notem_output[year])

        # Aggregate the dvector to the required segmentation
        if 'nhb' in self.notem_output[year]:
            tram_seg = nd.get_segmentation_level('nhb_p_m_ca')
        else:
            tram_seg = nd.get_segmentation_level('hb_p_m_ca')
        notem_tram_seg = notem_output_dvec.aggregate(tram_seg)

        return tram_data, notem_tram_seg

    def _tram_infill_msoa(self,
                          notem_tram_seg: nd.DVector,
                          tram_data: pd.DataFrame,
                          year: int,
                          tram_competitors: List[nd.Mode],
                          verbose: bool,
                          zone_col: str = 'msoa_zone_id',
                          mode_col: str = 'm',
                          val_col: str = 'val',
                          ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Tram infill for the MSOAs with tram data in the north.

        Parameters
        ----------
        notem_tram_seg:
            DVector containing notem trip end output in matching segmentation
            to tram_data
            
        tram_data:
            Dataframe containing tram data at MSOA level for the north.

        year:
            The year we're currently running for

        verbose:
            If set to True, it will print out progress updates while running.

        Returns
        -------
        notem_new_df:
            Returns the dataframe after tram infill.

        notem_msoa_wo_infill:
            Returns the dataframe before tram infill at msoa level.
        """
        # Init
        tram_data = tram_data.copy()

        # Converts DVector to dataframe
        notem_df = notem_tram_seg.to_df()

        for col, dt in self._target_col_dtypes['notem'].items():
            notem_df[col] = notem_df[col].astype(dt)

        notem_df = notem_df.rename(columns={val_col: self._val_col})

        # Retains only msoa zones that have tram data
        notem_df = notem_df.loc[notem_df[zone_col].isin(tram_data[zone_col])]
        notem_msoa_wo_infill = notem_df.copy()

        # Creates a subset of train mode
        train_mask = notem_df[mode_col] == nd.Mode.TRAIN.get_mode_num()
        notem_train = notem_df[train_mask].copy()

        # Calculates growth factor for tram based on rail growth
        if year == self.base_year:
            self.base_train = notem_train

        tram_data, growth_factors = self._grow_tram_by_rail(
            base_rail=self.base_train,
            future_rail=notem_train,
            base_tram=tram_data,
            future_tram_col='future_val',
        )

        # Replace val with future val
        tram_data = tram_data.drop(columns=self._val_col)
        tram_data = tram_data.rename(columns={'future_val': self._val_col})

        # TODO(BT, NK): Write out a report of the growth factors generated

        # Adds tram data to the notem dataframe
        notem_df = notem_df.append(tram_data)

        du.print_w_toggle("Starting tram infill for msoa zones with tram data...", verbose=verbose)

        # Infills tram data
        notem_new_df, more_tram_report = self._infill_internal(
            df=notem_df,
            tram_competitors=tram_competitors,
            non_val_cols=[self._zoning_system_col] + self._tram_segment_cols,
            mode_col=mode_col,
        )

        # TODO(BT, NK): Write out all places that tram is higher than train
        #  to a report using more_tram_report

        return notem_new_df, notem_msoa_wo_infill

    def _grow_tram_by_rail(self,
                           base_rail: pd.DataFrame,
                           future_rail: pd.DataFrame,
                           base_tram: pd.DataFrame,
                           future_tram_col: str = 'future_val',
                           ) -> pd.DataFrame:
        """
        Calculation of future year tram growth

        Parameters
        ----------
        base_rail:
            Dataframe containing base year rail data.

        future_rail:
            Dataframe containing future year rail data.

        base_tram:
            Dataframe containing base year tram data.

        Returns
        -------
        future_tram:
            base_tram with a new column added named future_tram_col. This
            will be the same as base_tram * growth_factors. Note that the
            return dataframe might not be in the same order as the
            passed in base_tram

        growth_factors:
            The growth factors used to generate future year tram
        """
        # TODO(BT): EFS style handling of growth
        # Init
        segment_cols = du.list_safe_remove(self._tram_segment_cols, ['m'])
        join_cols = [self._zoning_system_col] + segment_cols
        index_cols = join_cols + [self._val_col]

        # ## GET ALL DATA INTO ONE DF ## #
        # Tidy up
        def tidy_df(df, name):
            df = df.copy()
            df = df.drop(columns=['m'])
            df = pd_utils.reindex_cols(df, index_cols)
            df = df.sort_values(join_cols)
            df = df.rename(columns={self._val_col: name})
            return df

        base_rail = tidy_df(base_rail, 'base_rail')
        future_rail = tidy_df(future_rail, 'future_rail')
        base_tram = tidy_df(base_tram, 'base_tram')

        # Merge all together
        kwargs = {'on': join_cols, 'how': 'outer'}
        all_data = pd.merge(base_rail, future_rail, **kwargs).fillna(0)
        all_data = pd.merge(all_data, base_tram, **kwargs).fillna(0)

        # ## CALCULATE GROWTH FACTORS ## #
        all_data['rail_growth'] = np.divide(
            all_data['future_rail'].values,
            all_data['base_rail'].values,
            out=np.ones_like(all_data['base_rail'].values),
            where=all_data['base_rail'].values != 0,
        )

        # Calculate the future tram and tidy up
        all_data[future_tram_col] = all_data['base_tram'] * all_data['rail_growth']
        future_tram = pd_utils.reindex_cols(all_data, join_cols + ['base_tram', future_tram_col])
        future_tram = future_tram.rename(columns={'base_tram': self._val_col})
        future_tram['m'] = nd.Mode.TRAM.get_mode_num()

        # Extract the growth factors alone
        growth_df = pd_utils.reindex_cols(all_data, join_cols + ['rail_growth'])

        return future_tram, growth_df

    def _tram_infill_north(self,
                           notem_tram_seg: nd.DVector,
                           north_tram_data: pd.DataFrame,
                           tram_competitors: List[nd.Mode],
                           verbose: bool = True,
                           mode_col: str = 'm',
                           val_col: str = 'val',
                           ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Tram infill at the internal area (north area) level.

        Parameters
        ----------
        notem_tram_seg:
            DVector containing notem trip end output in revised segmentation(p,m,ca).

        tram_msoa:
            Dataframe containing tram data at msoa level for the north.

        verbose:
            If set to True, it will print out progress updates while
        running.

        Returns
        -------
        notem_new_df:
            Returns the dataframe after tram infill at north level.

        north_wo_infill:
            Returns the dataframe before tram infill at north level.
        """
        # Init
        north_tram_data = north_tram_data.copy()

        # Converts Dvector to ie sector level
        ie_sectors = nd.get_zoning_system('ie_sector')
        notem_df = notem_tram_seg.translate_zoning(ie_sectors).to_df()
        notem_df = notem_df.rename(columns={val_col: self._val_col})

        for col, dt in self._target_col_dtypes['notem_north'].items():
            notem_df[col] = notem_df[col].astype(dt)

        # Retains only internal (North) zones
        notem_df = notem_df.loc[notem_df['ie_sector_zone_id'] == 1]
        notem_df = notem_df.drop(columns=['ie_sector_zone_id'])
        north_wo_infill = notem_df.copy()

        # Tidy ready for infill
        group_cols = self._tram_segment_cols
        index_cols = group_cols + [self._val_col]

        notem_df = notem_df.reindex(columns=index_cols)
        notem_df = notem_df.groupby(group_cols).sum().reset_index()

        north_tram_data = north_tram_data.reindex(columns=index_cols)
        north_tram_data = north_tram_data.groupby(group_cols).sum().reset_index()

        df = pd.concat([notem_df, north_tram_data], ignore_index=True)

        du.print_w_toggle("Starting tram infill at north level...", verbose=verbose)
        # Infills tram data
        notem_new_df, more_tram_report = self._infill_internal(
            df=df,
            tram_competitors=tram_competitors,
            non_val_cols=self._tram_segment_cols,
            mode_col=mode_col,
        )

        # TODO(BT, NK): Write out all places that tram is higher than train
        #  to a report using more_tram_report

        return notem_new_df, north_wo_infill

    def _non_tram_infill(self,
                         north_wo_infill: pd.DataFrame,
                         north_w_infill: pd.DataFrame,
                         tram_zones: List[str],
                         dvec_tram_seg: nd.DVector,
                         tram_competitors: List[nd.Mode],
                         mode_col: str = 'm',
                         verbose: bool = True,
                         ):
        """
        Infills tram for MSOAs without tram data in the internal area (north area).

        Parameters
        ----------
        msoa_wo_infill:
            Dataframe of MSOA zones with tram data before infill.

        north_wo_infill:
            Dataframe of trip ends at internal(north) level before tram infill.

        msoa_w_infill:
            Dataframe of trip ends at MSOA after tram infill. Should be same data
            as msoa_wo_infill

        north_w_infill:
            Dataframe of trip ends at internal(north) level after tram infill.
            Should be same data as north_wo_infill

        tram_data:
            Dataframe of tram data at MSOA for the internal(north) area.

        notem_tram_seg:
            DVector of notem trip end output in tram segmentation(p,m,ca).

        verbose:
            If set to True, it will print out progress updates while running.

        Returns
        -------
        notem_df_new:
            Returns the dataframe after non-tram infill for msoa zones for
            all external area, but only non-tram zones for internal area
        """
        du.print_w_toggle("Starting tram adjustment for non tram areas...", verbose=verbose)
        # Init
        compet_mode_vals = [x.get_mode_num() for x in tram_competitors]
        non_val_cols = [self._zoning_system_col] + self._tram_segment_cols

        # ## STEP 1: CALCULATE NORTH AVERAGE MODE SHARE ADJUSTMENTS ## #
        # Set up dfs for merge
        north_w_infill = north_w_infill.copy()
        north_w_infill = north_w_infill.rename(columns={self._val_col: 'adj_val'})
        tram_mask = north_w_infill[mode_col] == nd.Mode.TRAM.get_mode_num()
        north_w_infill = north_w_infill[~tram_mask]

        # Stick into one df
        north_df = pd.merge(
            left=north_wo_infill,
            right=north_w_infill,
            how='outer',
            on=self._tram_segment_cols,
        ).fillna(0)

        # Filter down to just the competitor modes
        compet_mask = north_df[mode_col].isin(compet_mode_vals)
        north_df = north_df[compet_mask].copy()

        # Calculate the average adjustment factor
        north_df['adj_factor'] = north_df['adj_val'] / north_df[self._val_col]

        # Filter down to all we need
        cols = self._tram_segment_cols + ['adj_factor']
        north_adj_factors = pd_utils.reindex_cols(north_df, cols)

        # ## STEP 2. ADJUST NON-TRAM MSOA BY AVERAGE NORTH ADJUSTMENT ## #
        # TODO(BT): Make the zoning more flexible
        # Get the internal, external, and tram zones
        zoning_system = nd.get_zoning_system('msoa')
        internal_zones = zoning_system.internal_zones
        external_zones = zoning_system.external_zones

        # Split the original notem data into internal and external
        notem_df = dvec_tram_seg.to_df()

        external_mask = notem_df['msoa_zone_id'].isin(external_zones)
        ext_notem_df = notem_df[external_mask].copy()

        internal_mask = notem_df['msoa_zone_id'].isin(internal_zones)
        int_notem_df = notem_df[internal_mask].copy()

        # Filter down to non-tram zones
        tram_mask = int_notem_df['msoa_zone_id'].isin(tram_zones)
        int_no_tram_notem = int_notem_df[~tram_mask].copy()

        # Attach the avg north adj factors
        int_no_tram_notem = pd.merge(
            left=int_no_tram_notem,
            right=north_adj_factors,
            how='left',
            on=self._tram_segment_cols,
        ).fillna(1)

        # Adjust!
        int_no_tram_notem['new_val'] = int_no_tram_notem[self._val_col].copy()
        int_no_tram_notem['new_val'] *= int_no_tram_notem['adj_factor'].copy()
        int_no_tram_notem = int_no_tram_notem.drop(columns='adj_factor')

        # ## STEP 3. APPLY NEW MSOA MODE SHARES TO OLD TOTALS ## #
        # split into competitor and non-competitor
        compet_mask = int_no_tram_notem[mode_col].isin(compet_mode_vals)
        compet_df = int_no_tram_notem[compet_mask].copy()
        non_compet_df = int_no_tram_notem[~compet_mask].copy()

        # Calculate the new mode shares
        group_cols = du.list_safe_remove(non_val_cols, [mode_col])
        temp = compet_df.groupby(group_cols)
        compet_df['val_sum'] = temp[self._val_col].transform('sum')
        compet_df['new_val_sum'] = temp['new_val'].transform('sum')
        compet_df['new_mode_shares'] = compet_df['new_val'] / compet_df['new_val_sum']

        # Adjust new_vals to reflect old totals and new mode shares
        compet_df['new_val'] = compet_df['new_mode_shares'] * compet_df['val_sum']

        # Stick the competitor and non-competitor back together
        compet_df = pd_utils.reindex_cols(compet_df, list(non_compet_df))
        df_list = [compet_df, non_compet_df]
        int_no_tram_notem = pd.concat(df_list, ignore_index=True)

        # ## STEP 4. TIDY UP DFs. BRING EVERYTHING BACK TOGETHER ## #
        # Add the external back on
        ext_notem_df['new_val'] = ext_notem_df[self._val_col].copy()
        df_list = [int_no_tram_notem, ext_notem_df]
        no_tram_notem_df = pd.concat(df_list, ignore_index=True)

        # Check we haven't dropped anything!
        expected_total = no_tram_notem_df[self._val_col].values.sum()
        final_total = no_tram_notem_df['new_val'].values.sum()
        if not math_utils.is_almost_equal(expected_total, final_total):
            raise ValueError(
                "Some demand seems to have gone missing while infilling "
                "non tram zones!\n"
                "Starting demand: %s\n"
                "Ending demand: %s"
                % (expected_total, final_total)
            )

        new_df = no_tram_notem_df.drop(columns=[self._val_col])
        new_df = new_df.rename(columns={'new_val': self._val_col})
        new_df = new_df.sort_values(non_val_cols).reset_index(drop=True)

        return new_df, north_adj_factors

    def _combine_trips(self,
                       external_notem: pd.DataFrame,
                       tram_msoa: pd.DataFrame,
                       non_tram_msoa: pd.DataFrame,
                       year: int,
                       verbose: bool
                       ) -> nd.DVector:
        """
        Combines the tram infill with the external msoa zones and converts to Dvec.

        Parameters
        ----------
        external_notem:
            Dataframe containing trip end data of external msoa zones.

        tram_msoa:
            Dataframe containing tram infill data for 275 internal tram msoa zones.

        non_tram_msoa:
            Dataframe containing tram infill data for internal non tram msoa zones.

        year:
            The year for which the trip end data is processed.

        Returns
        -------
        notem_dvec:
            The updated Dvector after tram infill.

        """

        tram_msoa= tram_msoa.drop(['val'], axis=1)
        tram_msoa.rename(columns={'final_val': 'val'}, inplace=True)
        tram_msoa = pd_utils.reindex_and_groupby(tram_msoa,self._notem_cols, ['val'])
        final_df = external_notem.append([tram_msoa, non_tram_msoa], ignore_index=True)

        # Combine the data together
        final_df = pd_utils.reindex_and_groupby(final_df, self._notem_cols, ['val'])

        # Get the zoning system
        msoa_zoning = nd.get_zoning_system('msoa')

        # Get the appropriate segmentation level
        if 'nhb' in self.notem_output[year]:
            msoa_seg = nd.get_segmentation_level('nhb_p_m7_ca')
        else:
            msoa_seg = nd.get_segmentation_level('hb_p_m7_ca')

        # Create the revised DVec
        return nd.DVector(
            zoning_system=msoa_zoning,
            segmentation=msoa_seg,
            import_data=final_df,
            zone_col="msoa_zone_id",
            val_col="val",
            verbose=verbose,
        )

    def _infill_internal(self,
                         df: pd.DataFrame,
                         tram_competitors: List[nd.Mode],
                         non_val_cols: List[str],
                         mode_col: str = 'm',
                         ) -> pd.DataFrame:
        """
        Creates a subset of the given dataframe using a condition.

        Parameters
        ----------
        df:
            The original dataframe that needs to be subset.

        tram_competitors:
            The modes which are considered competitors of tram. These modes
            will be adjusted to infill the tram trips

        Returns
        -------
        notem_sub_df:
            Returns the subset of the original dataframe.

        more_tram_report:
            A report of all the places tram trips were higher than rail trips

        """
        # Init
        df = df.copy()
        df.reset_index(drop=True, inplace=True)

        # Create needed masks
        train_mask = df[mode_col] == nd.Mode.TRAIN.get_mode_num()
        tram_mask = df[mode_col] == nd.Mode.TRAM.get_mode_num()

        # Get just the train and tram trips
        train_df = df[train_mask].copy().reset_index(drop=True)
        tram_df = df[tram_mask].copy().reset_index(drop=True)

        # ## REMOVE TRAM TRIPS FROM TRAIN ## #
        common_cols = du.list_safe_remove(non_val_cols, [mode_col])

        # Combine into one df for comparisons
        train_df.drop(columns=mode_col, inplace=True)
        train_df.rename(columns={self._val_col: 'train'}, inplace=True)

        tram_df.drop(columns=mode_col, inplace=True)
        tram_df.rename(columns={self._val_col: 'tram'}, inplace=True)

        tram_train_df = pd.merge(
            left=train_df,
            right=tram_df,
            on=common_cols,
            how='outer',
        ).fillna(0)

        # Generate a report where there are more tram than train trips
        more_tram_than_train = tram_train_df['tram'] > tram_train_df['train']
        more_tram_report = tram_train_df[more_tram_than_train]
        removed_tram_trips = (more_tram_report['tram'] - more_tram_report['train']).values.sum()

        # TODO(BT): Report where tram is > 50% rail

        # Curtail tram trips where they're higher than train
        tram_train_df['new_tram'] = tram_train_df['tram'].mask(
            more_tram_than_train,
            tram_train_df['train'],
        )

        # Remove tram trips from train
        tram_train_df['new_train'] = tram_train_df['train'].copy()
        tram_train_df['new_train'] -= tram_train_df['tram']

        # Get back into input format
        train_cols = common_cols + ['train', 'new_train']
        train_df = pd_utils.reindex_cols(tram_train_df, train_cols)
        train_df['m'] = nd.Mode.TRAIN.get_mode_num()
        rename = {'train': self._val_col, 'new_train': 'new_val'}
        train_df.rename(columns=rename, inplace=True)

        tram_cols = common_cols + ['tram', 'new_tram']
        tram_df = pd_utils.reindex_cols(tram_train_df, tram_cols)
        tram_df['m'] = nd.Mode.TRAM.get_mode_num()
        rename = {'tram': self._val_col, 'new_tram': 'new_val'}
        tram_df.rename(columns=rename, inplace=True)

        # ## ADD EVERYTHING TO ONE DF READY FOR MODE SHARE ADJUSTMENT ## #
        # Get everything other than tram and train
        other_df = df[~(train_mask | tram_mask)].copy()
        other_df['new_val'] = other_df[self._val_col].copy()

        # Stick back together
        df_list = [other_df, train_df, tram_df]
        new_df = pd.concat(df_list, ignore_index=True)

        # ## ADJUST COMPETITOR MODES BACK TO ORIGINAL SHARES ##
        # Split into competitor and non-competitor dfs
        compet_mode_vals = [x.get_mode_num() for x in tram_competitors]
        compet_mask = new_df[mode_col].isin(compet_mode_vals)
        compet_df = new_df[compet_mask].copy()
        non_compet_df = new_df[~compet_mask].copy()

        # Calculate the old mode shares
        temp = compet_df.groupby(common_cols)
        compet_df['val_sum'] = temp[self._val_col].transform('sum')
        compet_df['new_val_sum'] = temp['new_val'].transform('sum')
        compet_df['old_mode_shares'] = compet_df[self._val_col] / compet_df['val_sum']

        # Adjust new_vals to reflect old mode shares
        compet_df['new_val'] = compet_df['old_mode_shares'] * compet_df['new_val_sum']

        # ## TIDY UP DF FOR RETURN ## #
        compet_df = compet_df.reindex(columns=list(non_compet_df))
        new_df = pd.concat([compet_df, non_compet_df], ignore_index=True)

        # Check we haven't dropped anything
        expected_total = df[~tram_mask][self._val_col].values.sum() - removed_tram_trips
        final_total = new_df['new_val'].values.sum()
        if not math_utils.is_almost_equal(expected_total, final_total):
            raise ValueError(
                "Some demand seems to have gone missing while infilling "
                "tram!\n"
                "Starting demand: %s\n"
                "Ending demand: %s"
                % (expected_total, final_total)
            )

        new_df = new_df.drop(columns=[self._val_col])
        new_df = new_df.rename(columns={'new_val': self._val_col})
        new_df = new_df.sort_values(non_val_cols).reset_index(drop=True)

        return new_df, more_tram_report
