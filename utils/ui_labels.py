LABEL_APPLICATION = "Application"
LABEL_APPLICATION_PLURAL = "Applications"
LABEL_APP_GROUP_INTERNAL_HINT = "Application (internally: Application)"
LABEL_APPLICATION_INSTANCE = "Application Instance"
LABEL_APPLICATION_INSTANCE_PLURAL = "Application Instances"
LABEL_PRIMARY_INSTANCE = "Primary Instance"


def app_group_label() -> str:
    return LABEL_APPLICATION


def app_instance_label() -> str:
    return LABEL_APPLICATION_INSTANCE

