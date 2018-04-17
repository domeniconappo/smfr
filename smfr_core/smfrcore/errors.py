class SMFRError(Exception):
    pass


class SMFRDBError(SMFRError):
    pass


class SMFRRestException(SMFRError):
    def __init__(self, response, status_code):
        err = response.get('error', {})
        message = '{}: ({})'.format(status_code, err.get('description', 'No details.'))
        super().__init__(message)
