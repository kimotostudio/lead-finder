from web_app import app as web_app


def test_score_range_min_only():
    assert web_app.score_in_range(50, 40, None)
    assert not web_app.score_in_range(30, 40, None)


def test_score_range_max_only():
    assert web_app.score_in_range(50, None, 60)
    assert not web_app.score_in_range(70, None, 60)


def test_score_range_both_bounds():
    assert web_app.score_in_range(50, 40, 60)
    assert not web_app.score_in_range(30, 40, 60)
    assert not web_app.score_in_range(70, 40, 60)


def test_score_range_empty_bounds():
    assert web_app.score_in_range(50, None, None)
    assert not web_app.score_in_range(None, None, None)

