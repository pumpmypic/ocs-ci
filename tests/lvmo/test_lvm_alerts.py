import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_lvm_not_installed,
)
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.exceptions import MetricFailCompareThinPool


log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["volume_mode", "volume_binding_mode"],
    argvalues=[
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.WFFC_VOLUMEBINDINGMODE],
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.WFFC_VOLUMEBINDINGMODE],
        ),
        pytest.param(
            *[constants.VOLUME_MODE_FILESYSTEM, constants.IMMEDIATE_VOLUMEBINDINGMODE],
        ),
        pytest.param(
            *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
        ),
    ],
)
class TestLvmCapacityAlerts(ManageTest):
    """
    Test Alerts when LVM capacity is exceeded 75%, 85%

    """

    access_mode = constants.ACCESS_MODE_RWO

    @pytest.fixture()
    def init_lvm(self):
        self.lvm = LVM()
        disk1 = self.lvm.pv_data["pv_list"][0]
        self.disk_size = self.lvm.pv_data[disk1]["pv_size"]
        self.thin_pool_size = float(self.lvm.get_thin_pool1_size())
        self.pvc_size = int(self.thin_pool_size)

    @pytest.fixture()
    def storageclass(self, lvm_storageclass_factory_class, volume_binding_mode):
        self.sc_obj = lvm_storageclass_factory_class(volume_binding_mode)

    @pytest.fixture()
    def namespace(self, project_factory_class):
        self.proj_obj = project_factory_class()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def pvc(self, pvc_factory_class, volume_mode, volume_binding_mode):
        self.status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            self.status = constants.STATUS_BOUND
        self.pvc_obj = pvc_factory_class(
            project=self.proj_obj,
            interface=None,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            status=self.status,
            access_mode=self.access_mode,
            volume_mode=volume_mode,
        )

    @pytest.fixture()
    def pod(self, pod_factory_class, volume_mode):
        self.block = False
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            self.block = True
        self.pod_obj = pod_factory_class(pvc=self.pvc_obj, raw_block_pv=self.block)

    @tier1
    @skipif_lvm_not_installed
    @skipif_ocs_version("<4.10")
    def test_thin_pool_capacity_alert(
        self,
        namespace,
        init_lvm,
        storageclass,
        # pvc,
        # pod,
        volume_mode,
        pvc_factory,
        pod_factory,
        volume_binding_mode,
    ):
        """

        Test to verify thin pool capacity alert:
        1. run io up to 70, check alerts - no alert expected
        2. run io up to 76, check alerts
        3. run io up to 86, check alerts - critical alert expected

        """
        log.info("Test Started successfully")
        log.info(f"LVMCluster version is {self.lvm.get_lvm_version()}")
        log.info(
            f"Lvm thin-pool overprovisionRation is {self.lvm.get_lvm_thin_pool_config_overprovision_ratio()}"
        )
        log.info(
            f"Lvm thin-pool sizePrecent is {self.lvm.get_lvm_thin_pool_config_size_percent()}"
        )
        size_to_70 = f"{int(float(self.thin_pool_size)*0.7)}Gi"
        size_to_76 = f"{int(float(self.thin_pool_size)*0.07)}Gi"
        size_to_86 = f"{int(float(self.thin_pool_size)*0.1)}Gi"
        sizes_list = [
            {
                "size_to_fill": size_to_70,
                "file_name": "run-to-70",
                "pvc_expected_size": f"{float(self.pvc_size)*0.7}",
                "alert": None,
            },
            {
                "size_to_fill": size_to_76,
                "file_name": "run-to-76",
                "pvc_expected_size": f"{float(self.pvc_size)*0.77}",
                "alert": constants.TOPOLVM_ALERTS.get("tp_data_75_precent"),
            },
            {
                "size_to_fill": size_to_86,
                "file_name": "run-to-86",
                "pvc_expected_size": f"{float(self.pvc_size)*0.87}",
                "alert": constants.TOPOLVM_ALERTS.get("tp_data_85_precent"),
            },
        ]

        log.info(f"LV Size:{self.thin_pool_size}")
        self.metric_data = dict()
        storage_type = "fs"
        if volume_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = "block"
        pvc_list = []
        pod_list = []
        for size in sizes_list:
            log.info(
                f"{size.get('size_to_fill')}, {size.get('file_name')}, {size.get('pvc_expected_size')}"
            )

            status = constants.STATUS_PENDING
            if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
                status = constants.STATUS_BOUND
            pvc_list.append(
                pvc_factory(
                    project=self.proj_obj,
                    interface=None,
                    storageclass=self.sc_obj,
                    size=self.pvc_size,
                    status=status,
                    access_mode=self.access_mode,
                    volume_mode=volume_mode,
                )
            )

            block = False
            if volume_mode == constants.VOLUME_MODE_BLOCK:
                block = True
            # io_pvc_now = pvc_list[-1]
            pod_list.append(pod_factory(pvc=pvc_list[-1], raw_block_pv=block))
            # io_pod_now = pod_list[-1]
            pod_list[-1].run_io(
                storage_type=storage_type,
                size=size.get("size_to_fill"),
                rw_ratio=0,
                jobs=1,
                runtime=0,
                depth=4,
                rate="1250m",
                rate_process=None,
                fio_filename=size.get("file_name"),
                bs="100M",
                end_fsync=0,
                invalidate=0,
                buffer_pattern=None,
                readwrite="write",
                direct=1,
                verify=False,
            )
            pod_list[-1].get_fio_results(timeout=1200)

            # Workaround for BZ-2108018
            status = constants.STATUS_PENDING
            if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
                status = constants.STATUS_BOUND
            minimal_pvc = pvc_factory(
                project=self.proj_obj,
                interface=None,
                storageclass=self.sc_obj,
                size="10",
                status=status,
                access_mode=self.access_mode,
                volume_mode=volume_mode,
                size_unit="Mi",
            )
            mini_pod = pod_factory(
                pvc=minimal_pvc, raw_block_pv=block, status=constants.STATUS_RUNNING
            )
            log.info(f"{mini_pod} created")
            time.sleep(60)
            # End of workaround
            size_without_gi = size["size_to_fill"].replace("Gi", "")
            self.lvm.compare_percent_data_from_pvc(pvc_list[-1], float(size_without_gi))
            val = self.lvm.parse_topolvm_metrics(constants.TOPOLVM_METRICS)
            self.metric_data = val
            log.info(self.metric_data)
            metric_result_verify = self.lvm.validate_thin_pool_data_percent_metric()
            if not metric_result_verify:
                raise MetricFailCompareThinPool(
                    f"Thin pool 1 percent is not aligned with"
                    f" topolvm_thinpool_data_percent metric"
                )

            log.info(f"getting alerts: {self.lvm.get_thin_provisioning_alerts()}")
            if size["file_name"] == "run-to-70":
                assert not self.lvm.check_for_alert(
                    size.get("alert")
                ), "Alert already exists"
            else:
                log.info(f"size: {size['file_name']}")
                assert self.lvm.check_for_alert(size.get("alert")), "Alert not found"
