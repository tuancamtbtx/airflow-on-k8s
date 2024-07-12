import os

from airlake.factory.entity import TeamConfig

test_team= TeamConfig(
    name="test",
    prefix="cdp",
    owner="test@gmail.vn",
    repo_id="",
    role_id=1,
    alert=None,
    conns=None,
    team_dir=os.path.join(os.path.dirname(__file__), 'cdp'),
)
