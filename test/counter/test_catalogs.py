import pytest
import time
import os

import sys
sys.path.insert(1, "../..")


@pytest.fixture(autouse=True) # задержка между тестами
def slow_down_tests():
    yield
    time.sleep(0.1)


@pytest.fixture(scope='session')
def catalogs():
    # для работы необходимо иметь файл settings.json с настройками доступа в каталоге с тестами
    from mediascope_api.counter import catalogs as cwc
    return cwc.CounterCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_get_adcampaigns(catalogs):
    assert not catalogs.get_adcampaigns(advertisement_ids=['9097213144252655872'], use_cache=False).empty


def test_get_areatype(catalogs):
    assert not catalogs.get_areatype().empty



