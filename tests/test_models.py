from flipping_cian.models import ParsedAdData


def test_parsed_ad_data_to_row_empty():
    data = ParsedAdData(url="http://example.com")
    row = data.to_row()
    # 22 columns expected
    assert len(row) == 22
    assert row[0] == "http://example.com"
    # All fields should be string or empty if not set (except parsed_at at 21)
    assert row[2] == ""
    assert isinstance(row[21], str)


def test_parsed_ad_data_to_row_full():
    data = ParsedAdData(
        url="http://example.com",
        publish_date="2024-01-01",
        price="10000000",
        title="1-room flat",
        address={
            "full": "Moscow",
            "district": "Arbat",
            "metro_station": "Arbatskaya",
            "okrug": "CAO",
        },
        description="Great flat",
        price_per_m2=200000,
        area=50.0,
        construction_year=2020,
        days_in_exposition=10,
        floor_info={"current": 5, "all": 10},
        housing_type="Secondary",
        renovation="Euro",
        rooms=1,
        total_views=100,
        unique_views=50,
        cian_id="12345",
        metro_walk_time=5,
    )
    row = data.to_row()
    assert len(row) == 22
    assert row[0] == "http://example.com"
    assert row[1] == "2024-01-01"
    assert row[2] == "10000000"
    assert row[3] == "1-room flat"
    assert row[4] == "Moscow"
    assert row[5] == "Great flat"
    assert row[6] == 200000.0
    assert row[7] == 50.0
    assert row[8] == 2020
    assert row[9] == 10
    assert row[10] == "Arbat"
    assert row[11] == "5/10"
    assert row[12] == "Secondary"
    assert row[13] == "Arbatskaya"
    assert row[14] == 5
    assert row[15] == "CAO"
    assert row[16] == "Euro"
    assert row[17] == 1
    assert row[18] == 100
    assert row[19] == 50
    assert row[20] == "12345"
