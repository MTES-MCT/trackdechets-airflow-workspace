# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access
from flask import Blueprint, Markup, send_from_directory
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

# Creating a flask blueprint to integrate the templates and static folder
bp_elementary = Blueprint(
    "elementary_plugin",
    __name__,
    template_folder="",
    static_folder="",
    static_url_path="/elementary",
)

bp_dbt_docs = Blueprint(
    "dbt_docs_plugin",
    __name__,
    template_folder="",
    static_folder="",
    static_url_path="/dbt_doc",
)


# Creating a flask appbuilder BaseView
class ElementaryAppBuilderBaseView(AppBuilderBaseView):
    default_view = "elementary"
    route_base = "/elementary"

    @expose("/elementary")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def elementary(self):
        with open("/opt/www/elementary/elementary.html") as f:
            html = f.read()
        return Markup(html)

    @expose("/<path:filename>")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def serve_static(self, filename):
        root_dir = "/opt/www/elementary"
        return send_from_directory(root_dir, filename)


class DbtDocAppBuilderBaseView(AppBuilderBaseView):
    default_view = "dbt_doc"
    route_base = "/dbt_doc"

    @expose("/dbt_doc")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def dbt_doc(self):
        with open("/opt/www/dbt/index.html") as f:
            html = f.read()
        return Markup(html)

    @expose("/<path:filename>")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def serve_static(self, filename):
        root_dir = "/opt/www/dbt"
        return send_from_directory(root_dir, filename)


v_appbuilder_view_elementary = ElementaryAppBuilderBaseView()
v_appbuilder_view_dbt_doc = DbtDocAppBuilderBaseView()
v_appbuilder_package_elementary = {
    "name": "Elementary",
    "category": "Utils",
    "view": v_appbuilder_view_elementary,
}

v_appbuilder_package_dbt = {
    "name": "dbt doc",
    "category": "Utils",
    "view": v_appbuilder_view_dbt_doc,
}

v_appbuilder_nomenu_package_elementary = {"view": v_appbuilder_view_elementary}
v_appbuilder_nomenu_package_dbt_doc = {"view": v_appbuilder_view_dbt_doc}


# Defining the plugin class
class AirflowElementaryPlugin(AirflowPlugin):
    name = "elementary_plugin"
    flask_blueprints = [bp_elementary, bp_dbt_docs]
    appbuilder_views = [
        v_appbuilder_package_elementary,
        v_appbuilder_nomenu_package_elementary,
        v_appbuilder_package_dbt,
        v_appbuilder_nomenu_package_dbt_doc,
    ]
