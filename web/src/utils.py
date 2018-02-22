from flask import flash


class MessageClass:
    ERROR = 'danger'
    INFO = 'info'
    WARNING = 'warning'
    SUCCESS = 'success'
    MESSAGE_CLASSES = (ERROR, INFO, WARNING, SUCCESS,)


def add_message(message, category):
    if category not in MessageClass.MESSAGE_CLASSES:
        category = 'info'
    flash(message, category)
