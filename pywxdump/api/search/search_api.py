from flask import Blueprint, g, Flask

from pywxdump.api import api
from pywxdump.api.rjson import ReJson, ok
from pywxdump.api.utils import error9999, read_session_local_wxid, read_session
from pywxdump.dbpreprocess.fts.parsingFTSFactroy import ParsingFTSFactory

app = Flask(__name__)
app.register_blueprint(api, url_prefix='/api')  # Ensure the url_prefix matches your route


