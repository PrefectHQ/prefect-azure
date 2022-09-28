from prefect_azure.aci import ACITask


def test_empty_list_command_validation():
    # ensure that the default command is set automatically if the user
    # provides an empty command list
    aci_flow_run = ACITask(command=[])
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()


def test_missing_command_validation():
    # ensure that the default command is set automatically if the user
    # provides None
    aci_flow_run = ACITask(command=None)
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()
