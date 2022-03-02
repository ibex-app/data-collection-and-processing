ibex_models.processor import Processor

from app.core.processors.tf_idf.tf_idf import TFIDFProcessor
from app.core.processors.speech_to_text.speech_to_text import SpeechToTextProcessor
from app.core.processors.ner.ner import NERProcessor
from app.core.processors.face_recognition.face_recognition import FaceRecognitionProcessor
from app.core.processors.sentiment.sentiment import SentimentProcessor
from app.core.processors.hate_speech.hate_speech import HateSpeechProcessor
from app.core.processors.topic.topic import TopicProcessor
from app.core.processors.botscore.botscore import BotscoreProcessor


processor_classes = {
    Processor.tf_idf : TFIDFProcessor,
    Processor.speech_to_text : SpeechToTextProcessor,
    Processor.ner : NERProcessor,
    Processor.face_recognition : FaceRecognitionProcessor,
    Processor.sentiment : SentimentProcessor,
    Processor.hate_speech : HateSpeechProcessor,
    Processor.topic : TopicProcessor,
    Processor.botscore : BotscoreProcessor
}
