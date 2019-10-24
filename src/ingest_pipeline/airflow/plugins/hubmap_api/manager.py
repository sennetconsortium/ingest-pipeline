import os
import logging
from io import StringIO

from werkzeug.exceptions import HTTPException, NotFound 

from flask import Blueprint, current_app, send_from_directory, abort, escape, render_template
from flask_admin import BaseView, expose

from jinja2 import TemplateNotFound

LOGGER = logging.getLogger(__name__)

def map_to_list(map):
    lst = []
    for elt in map.iter_rules():
        lst.append(elt)
    return lst


class APIAdminView1(BaseView):
    @expose('/')
    def api_admin_view1(self):
        LOGGER.info('In APIAdminView1.api_admin_view1')
        return show_template('generic.html',
                             title='Known Routes', 
                             content_lst=map_to_list(current_app.url_map))
aav1 = APIAdminView1(category='HuBMAP API', name="Known Routes")


class APIAdminView2(BaseView):
    @expose('/')
    def api_admin_view2(self):
        LOGGER.info('In APIAdminView1.api_admin_view2')
        return show_template('generic.html',
                             title='Flask Config', 
                             content_lst=["{0} = {1}".format(k, v) for k, v in current_app.config.items()])
aav2 = APIAdminView2(category='HuBMAP API', name="Flask Config")


# Create a Flask blueprint to hold the HuBMAP API
blueprint = Blueprint(
    "hubmap_api", __name__,
    url_prefix='/api/hubmap',
    template_folder='templates',
    static_folder='static',
)

@blueprint.route('/static/', defaults={'page':'index.html'})
@blueprint.route('/static/<page>')
def show_static(page):
    try:
        static_dir = os.path.join(os.path.dirname(__file__), 'static')
        return send_from_directory(static_dir, page)
    except NotFound as e:
        LOGGER.info('static page {0} not found: {1}'.format(page, repr(e)))
        abort(404)

@blueprint.route('/templates/', defaults={'page': 'index.html'})
@blueprint.route('/templates/<page>')
def show_template(page, title=None, content=None, content_lst=None):
    try:
        return render_template(page,
                               title=title,
                               content=content,
                               content_lst=content_lst)
    except TemplateNotFound as e:
        LOGGER.info('template page {0} not found: {1}'.format(page, e))
        abort(404)

