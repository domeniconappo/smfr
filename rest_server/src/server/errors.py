class SMFRError(Exception):
    pass


class SMFRRestException(SMFRError):
    def __init__(self, response):
        message = '{}: {} {}'.format(response.get('status'), response.get('title'), response.get('description'))
        super().__init__(message)


class SMFRDBError(SMFRError):
    pass


