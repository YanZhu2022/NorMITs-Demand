

import os

# Local imports
import normits_demand as nd
from normits_demand import efs_constants as consts
from normits_demand.utils import exceptional_growth as eg


efs = nd.ExternalForecastSystem(
    model_name=consts.MODEL_NAME,
    iter_num=1,
    import_home="Y:/",
    export_home="E:/",
    integrate_dlog=True,
    scenario_name=consts.SC04_UZC
)

generation_file = (
    "20201217 TfN EFS - Task D5 Bespoke Zone - MANSAM Example Inputs v1b.xlsx"
)

test_generations_path = (
    os.path.join(r"Y:\NorMITs Demand\inputs\MANSAM", generation_file)
)
eg.adjust_bespoke_zones(test_generations_path,
                        efs.exports,
                        efs.model_name,
                        base_year=consts.BASE_YEAR_STR,
                        recreate_donor=True,
                        audit_path=efs.exports["audits"])
