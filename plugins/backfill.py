from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from airlake.manager.backfill_web import Backfill

backfill_admin_view = {"category": "Admin", "name": "Backfill",  "view": Backfill()}


appbuilder_mitem = {"name": "Google",
                    "category": "Search",
                    "category_icon": "fa-th",
                    "href": "https://www.google.com"}

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='images',
    static_url_path='/static/images')


# Defining the plugin class
class AirflowBackfillPlugin(AirflowPlugin):
    name = "backfill"
    flask_blueprints = [bp]
    appbuilder_views = [backfill_admin_view]
    appbuilder_menu_items = [appbuilder_mitem]
