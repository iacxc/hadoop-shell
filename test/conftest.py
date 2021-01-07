import pytest

from clients import LivySession


@pytest.fixture(scope='session')
def g_livy():
    livy = LivySession.get_session(name='LivyTest')
    yield livy
    livy.delete()
