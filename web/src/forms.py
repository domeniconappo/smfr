from flask_wtf import FlaskForm, Form
from wtforms import StringField, RadioField, FileField, HiddenField, FieldList, FormField
from wtforms.fields.html5 import DateTimeField
from wtforms.validators import DataRequired, Optional


class TwitterKeysField(FlaskForm):
    consumer_key = StringField('Consumer Key')
    consumer_secret = StringField('Consumer Secret')
    access_token = StringField('Access Token')
    access_token_secret = StringField('Access Token Secret')

    def __init__(self, csrf_enabled=False, *args, **kwargs):
        super().__init__(csrf_enabled=False, *args, **kwargs)


class NewCollectorForm(FlaskForm):
    trigger = RadioField('Triggered', default='manual',
                         choices=[('manual', 'Manual'), ('background', 'Background'), ],
                         validators=[DataRequired('You must declare how collection was triggered.')]
                         )

    # config = FileField('Upload your config file', validators=(Optional(),))
    configuration = FormField(TwitterKeysField, label='Insert Twitter consumer keys')
    # kwfile = FileField('Upload keywords file', validators=(Optional(),))
    keywords = StringField('List of keywords comma separated')
    # locfile = FileField('Upload locations file', validators=(Optional(),))
    bounding_boxes = StringField('List of bounding boxes, comma separated')

    forecast_id = StringField('Forecast Id', validators=(Optional(),))
    runtime = DateTimeField('Run until...', format='%Y-%m-%d %H:%M', validators=(Optional(),))
    nuts2 = StringField('NUTS2 Code', validators=(Optional(),))
    tzclient = HiddenField('tzclient', validators=(Optional(),))


class ExportForm(FlaskForm):
    collection_id = HiddenField('Collection Id')
    ttype = RadioField('Tweet Status', default='collected',
                       choices=[('collected', 'Collected'), ('annotated', 'Annotated'), ('geotagged', 'Geotagged')],
                       )
    from_ts = DateTimeField('From...', format='%Y-%m-%d %H:%M', validators=(Optional(),))
    to_ts = DateTimeField('Until...', format='%Y-%m-%d %H:%M', validators=(Optional(),))
