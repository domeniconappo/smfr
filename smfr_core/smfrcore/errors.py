"""
SMFR common Errors
"""


class SMFRError(Exception):
    """
    Generic base Exception class
    """
    pass


class SMFRDBError(SMFRError):
    """
    DB errors
    """
    pass


class SMFRRestException(SMFRError):
    """
    REST API calls errors
    """
    def __init__(self, response, status_code):
        err = response.get('error', {})
        message = '{}: ({})'.format(status_code, err.get('description', 'No details.'))
        super().__init__(message)
