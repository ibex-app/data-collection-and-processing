from enum import Enum


class Processor(str, Enum):
    tf_idf = 'tf_idf'
    speech_to_text = 'speech_to_text'
    ner = 'ner'
    face_recognition = 'face_recognition'
    sentiment = 'sentiment'
    hate_speech = 'hate_speech'
    topic = 'topic'
    botscore = 'botscore'

