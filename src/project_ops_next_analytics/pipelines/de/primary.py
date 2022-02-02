# copyright 2020 quantumblack visual analytics limited
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# the software is provided "as is", without warranty of any kind,
# express or implied, including but not limited to the warranties
# of merchantability, fitness for a particular purpose, and
# noninfringement. in no event will the licensor or other contributors
# be liable for any claim, damages, or other liability, whether in an
# action of contract, tort or otherwise, arising from, out of, or in
# connection with the software or the use or other dealings in the software.
#
# the quantumblack visual analytics limited ("quantumblack") name and logo
# (either separately or in combination, "quantumblack trademarks") are
# trademarks of quantumblack. the license does not grant you any right or
# license to the quantumblack trademarks. you may not use the quantumblack
# trademarks or any confusingly similar mark as a trademark for your product,
# or use the quantumblack trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# see the license for the specific language governing permissions and
# limitations under the license.

"""example code for the nodes in the example pipeline. this code is meant
just for illustrating basic kedro features.

delete this when you start working on your own kedro project.
"""

from kedro.pipeline import Pipeline, node

from project_ops_next_analytics.nodes.primary.prm_test_chromatography_data import (
    prm_create_test_chromatography_data,
)
from project_ops_next_analytics.nodes.primary.prm_hr_data import prm_create_hr_data


from project_ops_next_analytics.nodes.primary.prm_batch_stage_data import (
    prm_create_batch_stage_data,
)
from project_ops_next_analytics.nodes.primary.prm_test_stability_results_data import (
    prm_create_test_stability_results_data,
)


from project_ops_next_analytics.nodes.primary.prm_stability_batch_data import (
    prm_create_stability_batch_data,
)

from project_ops_next_analytics.nodes.primary.prm_quality_batch_final_data import (
    prm_create_quality_batch_final_data,
)

from project_ops_next_analytics.nodes.primary.prm_quality_batch_stage_data import (
    prm_create_quality_batch_stage_data,
)

from project_ops_next_analytics.nodes.primary.prm_equipment_details import (
    prm_create_equipment_details_data,
)
from project_ops_next_analytics.nodes.primary.prm_tags_data import prm_create_tags_data
from project_ops_next_analytics.nodes.primary.prm_events_data import (
    prm_create_events_data,
)


def create_primary(**kwargs):
    pipeline = Pipeline(
        [
            node(prm_create_hr_data, "int_punchdetail_data", "prm_hr_data"),
            # node(
            #     prm_create_batch_stage_data,
            #     "int_mes_pasx_cpp_data",
            #     "prm_batch_stage_data",
            # ),
            # node(
            #     prm_create_stability_batch_data,
            #     "int_cqa_labware_batch_cl_data",
            #     "prm_stability_batch_data",
            # ),
            # node(
            #     prm_create_test_stability_results_data,
            #     "int_cqa_labware_stability_cl_data",
            #     "prm_test_stability_results_data",
            # ),
            # node(
            #     prm_create_test_chromatography_data,
            #     "int_empower_data",
            #     "prm_test_chromatography_data",
            # ),
            # node(
            #     prm_create_quality_batch_final_data,
            #     ["int_tfct_cqa_data", "int_genealogy_data"],
            #     "prm_quality_batch_final_data",
            # ),
            # node(
            #     prm_create_quality_batch_stage_data,
            #     ["int_cqa_labware_cl_data", "int_genealogy_data", "int_oot_data", "int_invalid_oos_data",],
            #     "prm_quality_batch_stage_data",
            # ),
            # node(
            #     prm_create_equipment_details_data,
            #     [
            #         "int_cpp_data",
            #         "int_ems_data",
            #         "int_equip_room_data",
            #         "int_breakdown_data",
            #         "int_pmp_data",
            #         "int_mes_pasx_ipc_data",
            #         "parameters",
            #     ],
            #     "prm_equipment_details_data",
            # ),
            # node(
            #     prm_create_events_data,
            #     [
            #         "int_breakdown_data",
            #         "int_pmp_data",
            #         "int_mes_pasx_ipc_data",
            #         "parameters",
            #     ],
            #     "prm_events_data",
            # ),
            # node(
            #     prm_create_tags_data,
            #     [
            #         "int_cpp_data",
            #         "int_ems_data",
            #         "int_equip_room_data",
            #         "parameters",
            #     ],
            #     "prm_tags_data",
            # ),
        ],
        tags="de",
    )
    return pipeline


primary_pipeline = Pipeline([create_primary()])
