def test_undertests():
    from smfrcore.utils import UNDER_TESTS
    assert UNDER_TESTS


def test_mysql():
    from smfrcore.models.sql import TwitterCollection
    res = TwitterCollection.query.all()
    assert len(res) == 0
