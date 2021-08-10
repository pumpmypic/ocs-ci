import logging
import time
import pytest
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.helpers.helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


class StorageClassUI(PageNavigator):
    """
    User Interface Selenium

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.sc_loc = locators[ocp_version]["storageclass1"]

    def create_rbd_storage_class(self):
        self.navigate_storageclasses_page()
        sc_name = create_unique_resource_name("test", "storageclass")
        self.do_click(self.sc_loc["create_storageclass1"])
        self.do_send_keys(self.sc_loc["storageclass_name1"], sc_name)
        self.do_click(self.sc_loc["storageclass_provisioner_dropdown1"])
        self.do_click(self.sc_loc["rbd_provisioner1"])
        self.do_click(self.sc_loc["select_pool_dropdown1"])
        self.do_click(self.sc_loc["pool_ocs_storagecluster_cephblockpool1"])
        self.do_click(self.sc_loc["save_storageclass1"])
        name_found = self.wait_until_expected_text_is_found(
                                                            locator=self.sc_loc["storage_class_heading_name"],
                                                            expected_text=sc_name,
                                                            timeout=5
        )
        if name_found:
            return sc_name
        else:
            self.take_screenshot()
            return None

