import pytest


# First test loads imports into global variables used by other tests, to prevent reloading of imports
def test_stitch_check_extractions_pass(load_data):
    global stitch_check_extractions
    from general.monitoring import stitch_check_extractions

    response = load_data('monitoring/extractions_success.json')
    failed_extrations = stitch_check_extractions(response)
    # Expected to return empty dict as no extractions failed
    assert failed_extrations == {}

def test_stitch_check_extractions_fail(load_data):
    response = load_data('monitoring/extractions_fail.json')
    failed_extractions = stitch_check_extractions(response)
    # Expected to return dict containing failed extractions
    assert failed_extractions == {99999: 'Terminated'}

def test_stitch_check_extractions_error():
    # Expected to raise exception since response body is empty
    with pytest.raises(TypeError):
        stitch_check_extractions({})

def test_stitch_check_loads_pass(load_data):
    global stitch_check_loads
    from general.monitoring import stitch_check_loads

    response = load_data('monitoring/loads_success.json')
    failed_loads = stitch_check_loads(response)
    # Expected to return empty dict as no loads failed
    assert failed_loads == {}

def test_stitch_check_loads_fail(load_data):
    response = load_data('monitoring/loads_fail.json')
    failed_loads = stitch_check_loads(response)
    # Expected to return dict containing failed loads
    assert failed_loads == {'raw_source_2': 'some_error_value'}

def test_stitch_check_loads_error(load_data):
    # Expected to raise exception since response body is empty
    with pytest.raises(TypeError):
        stitch_check_loads({})
    