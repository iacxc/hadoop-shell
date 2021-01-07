
import getpass


def test_user(g_livy):
    print(g_livy.session_id, g_livy.state)
    user = getpass.getuser()
    rr = g_livy.run_code('''
import getpass
user = getpass.getuser()
print(user)
%json user
''')
    print(rr)
    print(f'Time elapsed: {rr["completed"] - rr["started"]} seconds.')
    assert rr['output']['data']['application/json'] == user