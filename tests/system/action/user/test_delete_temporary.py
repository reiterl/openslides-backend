from tests.system.action.base import BaseActionTestCase


class UserDeleteTemporaryActionTest(BaseActionTestCase):
    def test_delete_correct(self) -> None:
        self.create_model("user/111", {"username": "username_srtgb123"})
        response = self.client.post(
            "/", json=[{"action": "user.delete_temporary", "data": [{"id": 111}]}],
        )

        self.assert_status_code(response, 200)
        self.assert_model_deleted("user/111")

    def test_delete_wrong_id(self) -> None:
        self.create_model("user/112", {"username": "username_srtgb123"})
        response = self.client.post(
            "/", json=[{"action": "user.delete_temporary", "data": [{"id": 111}]}],
        )
        self.assert_status_code(response, 400)
        model = self.get_model("user/112")
        assert model.get("username") == "username_srtgb123"
