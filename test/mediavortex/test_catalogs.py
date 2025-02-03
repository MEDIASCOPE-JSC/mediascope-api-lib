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
    from mediascope_api.mediavortex import catalogs as cwc
    return cwc.MediaVortexCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_get_tv_playbacktype(catalogs):
    assert not catalogs.get_tv_playbacktype().empty


def test_get_tv_platform(catalogs):
    assert not catalogs.get_tv_platform().empty


def test_load_tv_property(catalogs):
    assert not catalogs.load_tv_property().empty


def test_get_units(catalogs):
    assert len(catalogs.get_units()) > 0


def test_get_timeband_unit(catalogs):
    assert len(catalogs.get_timeband_unit()) > 0


def test_get_simple_unit(catalogs):
    assert len(catalogs.get_simple_unit()) > 0


def test_get_crosstab_unit(catalogs):
    assert len(catalogs.get_crosstab_unit()) > 0


def test_get_consumption_target_unit(catalogs):
    assert len(catalogs.get_consumption_target_unit()) > 0


def test_get_duplication_timeband_unit(catalogs):
    assert len(catalogs.get_duplication_timeband_unit()) > 0


def test_get_tv_subbrand(catalogs):
    assert not catalogs.get_tv_subbrand(ids=['3']).empty


def test_get_tv_subbrand_list(catalogs):
    assert not catalogs.get_tv_subbrand_list(ids=['7']).empty


def test_get_tv_research_day_type(catalogs):
    assert not catalogs.get_tv_research_day_type().empty


def test_get_tv_region(catalogs):
    assert not catalogs.get_tv_region().empty


def test_get_tv_program(catalogs):
    assert not catalogs.get_tv_program(ids=['8']).empty


def test_get_tv_program_type(catalogs):
    assert not catalogs.get_tv_program_type().empty


def test_get_tv_program_sport(catalogs):
    assert not catalogs.get_tv_program_sport().empty


def test_get_tv_program_sport_group(catalogs):
    assert not catalogs.get_tv_program_sport_group().empty


def test_get_tv_program_issue_description(catalogs):
    assert not catalogs.get_tv_program_issue_description(ids=['3']).empty


def test_get_tv_program_category(catalogs):
    assert not catalogs.get_tv_program_category().empty


def test_get_tv_net(catalogs):
    assert not catalogs.get_tv_net().empty


def test_get_tv_model(catalogs):
    assert not catalogs.get_tv_model(ids=['3']).empty


def test_get_tv_model_list(catalogs):
    assert not catalogs.get_tv_model_list(ids=['1']).empty


def test_get_tv_location(catalogs):
    assert not catalogs.get_tv_location().empty


def test_get_tv_grp_type(catalogs):
    assert not catalogs.get_tv_grp_type().empty


def test_get_tv_exchange_rate(catalogs):
    assert not catalogs.get_tv_exchange_rate().empty


def test_get_tv_day_week(catalogs):
    assert not catalogs.get_tv_day_week().empty


def test_get_tv_company(catalogs):
    assert not catalogs.get_tv_company().empty


def test_get_tv_company_merge(catalogs):
    assert not catalogs.get_tv_company_merge().empty


def test_get_tv_calendar(catalogs):
    assert not catalogs.get_tv_calendar(research_date='2024-01-01').empty


def test_get_tv_breaks(catalogs):
    assert not catalogs.get_tv_breaks(ids=['2']).empty


def test_get_tv_brand(catalogs):
    assert not catalogs.get_tv_brand(ids=['1']).empty


def test_get_tv_brand_list(catalogs):
    assert not catalogs.get_tv_brand_list(ids=['1']).empty


def test_get_tv_article(catalogs):
    assert not catalogs.get_tv_article().empty


def test_get_tv_article_list4(catalogs):
    assert not catalogs.get_tv_article_list4(ids=['1']).empty


def test_get_tv_article_list3(catalogs):
    assert not catalogs.get_tv_article_list3(ids=['1']).empty


def test_get_tv_article_list2(catalogs):
    assert not catalogs.get_tv_article_list2(ids=['1']).empty


def test_get_tv_appendix(catalogs):
    assert not catalogs.get_tv_appendix(ids=['153560']).empty


def test_get_tv_advertiser(catalogs):
    assert not catalogs.get_tv_advertiser(ids=['1']).empty


def test_get_tv_advertiser_list(catalogs):
    assert not catalogs.get_tv_advertiser_list(ids=['1']).empty


def test_get_tv_advertiser_tree(catalogs):
    assert not catalogs.get_tv_advertiser_tree(advertiser_ids=['1']).empty


def test_get_tv_ad(catalogs):
    assert not catalogs.get_tv_ad(ids=['153560']).empty


def test_get_tv_ad_type(catalogs):
    assert not catalogs.get_tv_ad_type().empty


def test_get_tv_ad_style(catalogs):
    assert not catalogs.get_tv_ad_style().empty


def test_get_tv_ad_slogan_video(catalogs):
    assert not catalogs.get_tv_ad_slogan_video(ids=['1']).empty


def test_get_tv_ad_slogan_audio(catalogs):
    assert not catalogs.get_tv_ad_slogan_audio(ids=['1']).empty


def test_get_tv_time_band(catalogs):
    assert not catalogs.get_tv_time_band().empty


def test_get_tv_stat(catalogs):
    assert not catalogs.get_tv_stat().empty


def test_get_tv_relation(catalogs):
    assert len(catalogs.get_tv_relation()) > 0


def test_get_tv_monitoring_type(catalogs):
    assert not catalogs.get_tv_monitoring_type().empty


def test_get_tv_demo_attribute(catalogs):
    assert not catalogs.get_tv_demo_attribute().empty


def test_get_tv_program_country(catalogs):
    assert not catalogs.get_tv_program_country(ids=['2']).empty


def test_get_tv_company_holding(catalogs):
    assert not catalogs.get_tv_company_holding().empty


def test_get_tv_company_media_holding(catalogs):
    assert not catalogs.get_tv_company_media_holding().empty


def test_get_tv_thematic(catalogs):
    assert not catalogs.get_tv_thematic().empty


def test_get_tv_program_producer_country(catalogs):
    assert not catalogs.get_tv_program_producer_country().empty


def test_get_tv_prime_time_status(catalogs):
    assert not catalogs.get_tv_prime_time_status().empty


def test_get_tv_issue_status(catalogs):
    assert not catalogs.get_tv_issue_status().empty


def test_get_tv_breaks_style(catalogs):
    assert not catalogs.get_tv_breaks_style().empty


def test_get_tv_breaks_position(catalogs):
    assert not catalogs.get_tv_breaks_position().empty


def test_get_tv_breaks_distribution(catalogs):
    assert not catalogs.get_tv_breaks_distribution().empty


def test_get_tv_breaks_content(catalogs):
    assert not catalogs.get_tv_breaks_content().empty


def test_get_tv_area(catalogs):
    assert not catalogs.get_tv_area().empty


def test_get_tv_ad_position(catalogs):
    assert not catalogs.get_tv_ad_position().empty


def test_get_tv_company_status(catalogs):
    assert not catalogs.get_tv_company_status().empty


def test_get_tv_program_producer(catalogs):
    assert not catalogs.get_tv_program_producer().empty


def test_get_tv_program_group(catalogs):
    assert not catalogs.get_tv_program_group().empty


def test_get_tv_no_yes_na(catalogs):
    assert not catalogs.get_tv_no_yes_na().empty


def test_get_tv_language(catalogs):
    assert not catalogs.get_tv_language().empty


def test_get_tv_company_group(catalogs):
    assert not catalogs.get_tv_company_group().empty


def test_get_tv_company_category(catalogs):
    assert not catalogs.get_tv_company_category().empty


def test_get_tv_age_restriction(catalogs):
    assert not catalogs.get_tv_age_restriction().empty


def test_get_availability_period(catalogs):
    assert not catalogs.get_availability_period().empty


# def test_get_respondent_analysis_unit(catalogs):
#     assert len(catalogs.get_respondent_analysis_unit()) > 0


def test_get_tv_monitoring_cities(catalogs):
    assert not catalogs.get_tv_monitoring_cities().empty
