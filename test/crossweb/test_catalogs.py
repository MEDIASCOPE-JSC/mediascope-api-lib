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
    from mediascope_api.crossweb import catalogs as cwc
    return cwc.CrossWebCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_load_property(catalogs):
    assert not catalogs.load_property().empty


def test_load_media_property(catalogs):
    assert not catalogs.load_media_property().empty


def test_load_monitoring_property(catalogs):
    assert not catalogs.load_monitoring_property().empty


def test_load_media_duplication_property(catalogs):
    assert not catalogs.load_media_duplication_property().empty


def test_load_profile_duplication_property(catalogs):
    assert not catalogs.load_profile_duplication_property().empty


def test_get_media(catalogs):
    assert not catalogs.get_media(product_ids=['20822'], use_cache=False).empty


def test_get_theme(catalogs):
    assert not catalogs.get_theme(product_ids=['20822'], use_cache=False).empty


def test_get_resource_theme(catalogs):
    assert not catalogs.get_resource_theme(product_ids=['20822'], use_cache=False).empty


def test_get_holding(catalogs):
    assert not catalogs.get_holding(product_ids=['20822'], use_cache=False).empty


def test_get_resource(catalogs):
    assert not catalogs.get_resource(product_ids=['20822'], use_cache=False).empty


def test_get_product(catalogs):
    assert not catalogs.get_product(product_ids=['20822'], use_cache=False).empty


def test_get_ad_agency(catalogs):
    assert not catalogs.get_ad_agency(agency_ids=['59418'], use_cache=False).empty


def test_get_brand(catalogs):
    assert not catalogs.get_brand(brand_ids=['123483512'], use_cache=False).empty


def test_get_ad_campaign(catalogs):
    assert not catalogs.get_ad_campaign(campaign_ids=['6639030347009795136'], use_cache=False).empty


def test_get_ad(catalogs):
    assert not catalogs.get_ad(ad_ids=['5476534523671989057'], use_cache=False).empty


def test_get_media_unit(catalogs):
    assert len(catalogs.get_media_unit()) > 0


def test_get_ad_unit(catalogs):
    assert len(catalogs.get_ad_unit()) > 0


def test_get_media_total_unit(catalogs):
    assert len(catalogs.get_media_total_unit()) > 0


def test_get_monitoring_unit(catalogs):
    assert len(catalogs.get_monitoring_unit()) > 0


def test_get_media_duplication_unit(catalogs):
    assert len(catalogs.get_media_duplication_unit()) > 0


def test_get_profile_duplication_unit(catalogs):
    assert len(catalogs.get_profile_duplication_unit()) > 0


def test_get_media_profile_unit(catalogs):
    assert len(catalogs.get_media_profile_unit()) > 0


def test_get_usetype(catalogs):
    assert not catalogs.get_usetype().empty


def test_get_date_range(catalogs):
    assert not catalogs.get_date_range().empty


def test_get_product_brand(catalogs):
    assert not catalogs.get_product_brand(product_brand_ids=['399'], use_cache=False).empty


def test_get_product_category_l1(catalogs):
    assert not catalogs.get_product_category_l1(product_category_l1_ids=['1'], use_cache=False).empty


def test_get_product_category_l2(catalogs):
    assert not catalogs.get_product_category_l2(product_category_l2_ids=['60'], use_cache=False).empty


def test_get_product_category_l3(catalogs):
    assert not catalogs.get_product_category_l3(product_category_l3_ids=['128'], use_cache=False).empty


def test_get_product_category_l4(catalogs):
    assert not catalogs.get_product_category_l4(product_category_l4_ids=['4667'], use_cache=False).empty


def test_get_product_model(catalogs):
    assert not catalogs.get_product_model(advertiser_ids=['4926'], use_cache=False).empty


def test_get_product_subbrand(catalogs):
    assert not catalogs.get_product_subbrand(advertiser_ids=['4926'], use_cache=False).empty


def test_get_ad_network(catalogs):
    assert not catalogs.get_ad_network().empty


def test_get_ad_placement(catalogs):
    assert not catalogs.get_ad_placement().empty


def test_get_ad_player(catalogs):
    assert not catalogs.get_ad_player().empty


def test_get_ad_server(catalogs):
    assert not catalogs.get_ad_server().empty


def test_get_ad_source_type(catalogs):
    assert not catalogs.get_ad_source_type().empty


def test_get_ad_video_utility(catalogs):
    assert not catalogs.get_ad_video_utility().empty


def test_get_product_advertiser(catalogs):
    assert not catalogs.get_product_advertiser(advertiser_ids=[859236], use_cache=False).empty


def test_get_monitoring(catalogs):
    assert not catalogs.get_monitoring(advertiser_ids=['4926'], use_cache=False).empty


def test_get_product_category_tree(catalogs):
    assert not catalogs.get_product_category_tree(advertiser_ids=['4926'], use_cache=False).empty


def test_get_media_usetype(catalogs):
    assert not catalogs.get_media_usetype().empty


def test_get_media_total_usetype(catalogs):
    assert not catalogs.get_media_total_usetype().empty


def test_get_media_duplication_usetype(catalogs):
    assert True #not catalogs.get_media_duplication_usetype().empty


def test_get_profile_usetype(catalogs):
    assert not catalogs.get_profile_usetype().empty


def test_get_profile_duplication_usetype(catalogs):
    assert not catalogs.get_profile_duplication_usetype().empty


def test_get_monitoring_usetype(catalogs):
    assert not catalogs.get_monitoring_usetype().empty


def test_get_ad_list(catalogs):
    assert not catalogs.get_ad_list(ad_ids=['5476534523671989057'], use_cache=False).empty


def test_get_monitoring_theme(catalogs):
    assert not catalogs.get_monitoring_theme(product_ids=['23005'], use_cache=False).empty


def test_get_monitoring_resource_theme(catalogs):
    assert not catalogs.get_monitoring_resource_theme(product_ids=['23005'], use_cache=False).empty


def test_get_monitoring_holding(catalogs):
    assert not catalogs.get_monitoring_holding(product_ids=['23005'], use_cache=False).empty


def test_get_monitoring_resource(catalogs):
    assert not catalogs.get_monitoring_resource(product_ids=['23005'], use_cache=False).empty


def test_get_monitoring_product(catalogs):
    assert not catalogs.get_monitoring_product(product_ids=['23005'], use_cache=False).empty


def test_get_monitoring_media(catalogs):
    assert not catalogs.get_monitoring_media(product_ids=['23005'], use_cache=False).empty
