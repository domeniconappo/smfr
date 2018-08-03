from flask_wtf import FlaskForm, Form
from wtforms import StringField, RadioField, FileField, HiddenField, FieldList, FormField, FloatField
from wtforms.fields.html5 import DateTimeField
from wtforms.validators import DataRequired, Optional


class TwitterKeysField(FlaskForm):
    consumer_key = StringField('Consumer Key', validators=(Optional(),))
    consumer_secret = StringField('Consumer Secret', validators=(Optional(),))
    access_token = StringField('Access Token', validators=(Optional(),))
    access_token_secret = StringField('Access Token Secret', validators=(Optional(),))

    def __init__(self, csrf_enabled=False, *args, **kwargs):
        super().__init__(csrf_enabled=False, *args, **kwargs)


class BoundingBoxField(FlaskForm):
    min_lon = FloatField('Minimum Longitude', validators=(Optional(),))
    min_lat = FloatField('Minimum Latitude', validators=(Optional(),))
    max_lon = FloatField('Maximum Longitude', validators=(Optional(),))
    max_lat = FloatField('Maximum Latitude', validators=(Optional(),))

    def __init__(self, csrf_enabled=False, *args, **kwargs):
        super().__init__(csrf_enabled=False, *args, **kwargs)


class NewCollectorForm(FlaskForm):
    trigger = RadioField('Triggered', default='manual',
                         choices=[('manual', 'Manual'), ('background', 'Background'), ],
                         validators=[DataRequired('You must declare how collection was triggered.')]
                         )

    configuration = FormField(TwitterKeysField, label='Twitter consumer keys')
    keywords = StringField('List of keywords comma separated', validators=(Optional(),))
    bounding_box = FormField(BoundingBoxField, label='Bounding box coordinates')

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
