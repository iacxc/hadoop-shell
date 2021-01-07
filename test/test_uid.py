
import os


def test_uid(g_livy):
    print(g_livy.session_id, g_livy.state)
    uid = os.getuid()
    rr = g_livy.run_code('''
import os
uid = os.getuid()
print(uid)
%json uid
''')
    print(rr)
    print(f'Time elapsed: {rr["completed"] - rr["started"]} seconds.')
    assert rr['output']['data']['application/json'] == uid
