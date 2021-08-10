import logging
#import pytest

from ocs_ci.framework.testlib import ManageTest, ui
from ocs_ci.ocs.ui.storageclass_ui import StorageClassUI
#from ocs_ci.ocs.ocp import OCP
#from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)

class TestStorageClassUI(ManageTest):

    @ui
    def test_create_storageclass_rbd(self, setup_ui):
        storageclass_ui_obj = StorageClassUI(setup_ui)
        sc_name = storageclass_ui_obj.create_rbd_storage_class()
        if sc_name is not None:
            logger.info(f"Created storageclass with name {sc_name}")




