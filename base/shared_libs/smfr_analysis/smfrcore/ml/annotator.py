import os

import sklearn
from keras.models import load_model
from keras_preprocessing.sequence import pad_sequences

from smfrcore.utils.text import create_text_for_cnn
from smfrcore.models.cassandra import Tweet

from .helpers import models_path, models


class Annotator:

    @classmethod
    def load_annotation_model(cls, lang):
        """
        Load model and tokenizer for language lang
        :param lang: two characters language code (e.g. 'en')
        :return: Tuple (model, tokenizer)
        """
        tokenizer_path = os.path.join(models_path, models[lang] + '.tokenizer')
        tokenizer = sklearn.externals.joblib.load(tokenizer_path)
        tokenizer.oov_token = None
        model_path = os.path.join(models_path, models[lang] + '.model.h5')
        model = load_model(model_path)
        return model, tokenizer

    @classmethod
    def annotate_tweet(cls, tweet, flood_probability):
        tweet.annotations = {'flood_probability': ('yes', flood_probability)}
        tweet.ttype = Tweet.ANNOTATED_TYPE
        return tweet

    @classmethod
    def annotate(cls, model, tweets, tokenizer):
        """
        Annotate the tweet t using model and tokenizer

        :param model: CNN model used for prediction
        :param tweets: list of smfrcore.models.Tweet objects
        :param tokenizer:
        :return:
        """
        texts = (create_text_for_cnn(t.original_tweet_as_dict, []) for t in tweets)
        sequences = tokenizer.texts_to_sequences(texts)
        data = pad_sequences(sequences, maxlen=model.layers[0].input_shape[1])
        predictions_list = model.predict(data)
        predictions = predictions_list[:, 1]
        res = [cls.annotate_tweet(t, 1. * predictions[i]) for i, t in enumerate(tweets)]
        return res
