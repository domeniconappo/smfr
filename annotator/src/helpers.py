import os
import logging
from subprocess import Popen, PIPE

import ujson


CNN_MAX_SEQUENCE_LENGTH = 100

models_path = os.path.join(os.environ.get('MODELS_PATH', '/'), 'models')
current_models_mapping = os.path.join(models_path, 'current-model.json')
logger = logging.getLogger(__name__)


def rchop(str_, ending):
    if str_.endswith(ending):
        return str_[:-len(ending)]
    return str_


def models_by_language(path):
    res = {}
    if os.path.exists(path):
        with open(path) as f:
            res = ujson.load(f)
    return res.get('model-by-language', {})


def update_models():

    git_command = ['/usr/bin/git', 'pull', 'origin', 'master']
    repository = os.path.join(models_path, '../')

    git_query = Popen(git_command, cwd=repository, stdout=PIPE, stderr=PIPE)
    git_status, error = git_query.communicate()
    logger.info(git_status)
    logger.info(error)
    return models_by_language(current_models_mapping)


models = update_models()
