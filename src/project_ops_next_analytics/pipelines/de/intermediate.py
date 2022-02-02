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
from project_ops_next_analytics.nodes.intermediate.int_ems_data import (
    int_create_ems_data,
)
from project_ops_next_analytics.nodes.intermediate.int_breakdown_data import (
    int_create_breakdown_data,
)
from project_ops_next_analytics.nodes.intermediate.int_pmp_data import (
    int_create_pmp_data,
)


from project_ops_next_analytics.nodes.intermediate.int_mes_pasx_cpp_data import (
    int_create_mes_pasx_cpp_data,
)
from project_ops_next_analytics.nodes.intermediate.int_mes_pasx_ipc_data import (
    int_create_mes_pasx_ipc_data,
)
from project_ops_next_analytics.nodes.intermediate.int_equip_room_data import (
    int_create_equip_room_data,
)


from project_ops_next_analytics.nodes.intermediate.int_tfct_cqa_data import (
    int_create_tfct_cqa_data,
)
from project_ops_next_analytics.nodes.intermediate.int_cqa_cma_data import (
    int_create_cqa_cma_data,
)
from project_ops_next_analytics.nodes.intermediate.int_cqa_labware_cl import (
    int_create_cqa_labware_cl_data,
)
from project_ops_next_analytics.nodes.intermediate.int_cqa_labware_stability_cl import (
    int_create_cqa_labware_stability_cl_data,
)
from project_ops_next_analytics.nodes.intermediate.int_cqa_labware_batch_cl import (
    int_create_cqa_labware_batch_cl_data,
)


from project_ops_next_analytics.nodes.intermediate.int_genealogy_data import (
    int_create_genealogy_data,
)

from project_ops_next_analytics.nodes.intermediate.int_hr_data import (
    int_create_active_emp_data,
)
from project_ops_next_analytics.nodes.intermediate.int_hr_data import (
    int_create_exit_emp_data,
)
from project_ops_next_analytics.nodes.intermediate.int_hr_data import (
    int_create_transfer_emp_data,
)
from project_ops_next_analytics.nodes.intermediate.int_invalid_oos_data import (
    int_create_invalid_oos_data,
)
from project_ops_next_analytics.nodes.intermediate.int_oot_data import (
    int_create_oot_data,
)
from project_ops_next_analytics.nodes.intermediate.int_attendance_data import (
    int_create_attendance_data,
)
from project_ops_next_analytics.nodes.intermediate.int_empower_data import (
    int_create_empower_data,
)
from project_ops_next_analytics.nodes.intermediate.int_cpp_data import (
    int_create_cpp_data,
)
from project_ops_next_analytics.nodes.intermediate.int_invalid_oos_oot_data import (
    int_create_invalid_oos_oot_data,
)
from project_ops_next_analytics.nodes.intermediate.int_punchdetail_data import (
    int_create_punchdetail_data,
)


def create_intermediate(**kwargs):
    pipeline = Pipeline(
        [
            # node(int_create_ems_data, "raw_ems_data", "int_ems_data"),
            node(int_create_breakdown_data, "raw_breakdown_data", "int_breakdown_data"),
            node(int_create_pmp_data, "raw_pmp_data", "int_pmp_data"),
            # node(
            #     int_create_mes_pasx_cpp_data,
            #     ["raw_mes_pasx_cpp_data", "parameters",],
            #     "int_mes_pasx_cpp_data",
            # ),
            # node(
            #     int_create_mes_pasx_ipc_data,
            #     ["raw_mes_pasx_ipc_data", "parameters",],
            #     "int_mes_pasx_ipc_data",
            # ),
            # node(
            #     int_create_equip_room_data,
            #     "raw_equip_room_data",
            #     "int_equip_room_data",
            # ),
            # node(
            #     int_create_cqa_cma_data,
            #     ["raw_cqa_cma_data", "parameters"],
            #     "int_cqa_cma_data",
            # ),
            # node(
            #     int_create_tfct_cqa_data,
            #     ["raw_tfct_cqa_data", "parameters"],
            #     "int_tfct_cqa_data",
            # ),
            # node(
            #     int_create_cqa_labware_stability_cl_data,
            #     ["raw_cqa_labware_stability_cl_data", "parameters"],
            #     "int_cqa_labware_stability_cl_data",
            # ),
            # node(
            #     int_create_cqa_labware_batch_cl_data,
            #     ["raw_cqa_labware_batch_cl_data", "parameters"],
            #     "int_cqa_labware_batch_cl_data",
            # ),
            # node(
            #     int_create_cqa_labware_cl_data,
            #     ["raw_cqa_labware_cl_data", "parameters"],
            #     "int_cqa_labware_cl_data",
            # ),
            # node(int_create_genealogy_data, "raw_genealogy_data", "int_genealogy_data"),
            # node(
            #     int_create_punchdetail_data,
            #     "raw_punchdetail_data",
            #     "int_punchdetail_data",
            # ),
            # node(int_create_exit_emp_data, "raw_exit_emp_data", "int_exit_emp_data"),
            # node(
            #     int_create_active_emp_data, "raw_active_emp_data", "int_active_emp_data"
            # ),
            # node(
            #     int_create_transfer_emp_data,
            #     "raw_transfer_emp_data",
            #     "int_transfer_emp_data",
            # ),
            # node(
            #     int_create_attendance_data,
            #     "raw_attendance_data",
            #     "int_attendance_data",
            # ),
            # node(
            #     int_create_invalid_oos_data,
            #     "raw_invalid_oos_data",
            #     "int_invalid_oos_data",
            # ),
            # node(
            #     int_create_oot_data,
            #     "raw_oot_data",
            #     "int_oot_data",
            # ),
            # node(
            #     int_create_invalid_oos_oot_data,
            #     "raw_invalid_oos_oot_data",
            #     "int_invalid_oos_oot_data",
            # ),
            # node(int_create_empower_data, "raw_empower_data", "int_empower_data",),
            # node(int_create_cpp_data, ["raw_cpp_data", "parameters"], "int_cpp_data",),
        ],
        tags="de",
    )
    return pipeline


intermediate_pipeline = Pipeline([create_intermediate()])
