from flask_wtf import FlaskForm
from wtforms import StringField, RadioField, FileField, DateTimeField, SubmitField
from wtforms.validators import DataRequired, Optional


class NewCollectorForm(FlaskForm):
    trigger = RadioField('Triggered', default='background',
                         choices=[('background', 'Background'), ('manual', 'Manual'), ('on-demand', 'On Demand')],
                         validators=[DataRequired('You must declare how collection was triggered.')]
                         )
    # collection_type = RadioField(
    #     'Collection Type', default='keywords',
    #     choices=[('keywords', 'Keywords'), ('geo', 'Geotagged')],
    #     validators=[DataRequired('You must declare type of collection.')]
    # )
    config = FileField('Upload your config file', validators=[DataRequired('Collector configuration is mandatory.')])
    kwfile = FileField('Upload keywords file')
    locfile = FileField('Upload locations file')
    forecast_id = StringField('Forecast Id')
    runtime = DateTimeField('Run until...', format='%Y%m%d%H%M', validators=(Optional(),))
    nuts3 = StringField('NUTS3 Code')
    nuts3source = StringField('NUTS3 Source')
