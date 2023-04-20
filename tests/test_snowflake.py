from snowflake import SnowflakeGenerator


def test_dummy():
    assert True


def test_snowflake_generator():
    sf_gen = SnowflakeGenerator()
    next_sf_id = next(sf_gen)

    assert next_sf_id != 0
