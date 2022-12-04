import imp
from ibex_models import Processor

from app.core.processors.tf_idf.tf_idf import TFIDFProcessor
from app.core.processors.speech_to_text.speech_to_text import SpeechToTextProcessor
from app.core.processors.ner.ner import NERProcessor
from app.core.processors.face_recognition.face_recognition import FaceRecognitionProcessor
from app.core.processors.sentiment.sentiment import SentimentProcessor
from app.core.processors.hate_speech.hate_speech import HateSpeechProcessor
from app.core.processors.topic.topic import TopicProcessor
from app.core.processors.botscore.botscore import BotscoreProcessor
from app.core.processors.top_engagement.top_engagement import TopEngagementProcessor
from app.core.processors.detect_search_terms.detect_search_terms import DetectSearchTerms  
from app.core.processors.detect_language.detect_language import DetectLanguage


processor_classes = {
    Processor.tf_idf : TFIDFProcessor,
    Processor.speech_to_text : SpeechToTextProcessor,
    Processor.ner : NERProcessor,
    Processor.face_recognition : FaceRecognitionProcessor,
    Processor.sentiment : SentimentProcessor,
    Processor.hate_speech : HateSpeechProcessor,
    Processor.topic : TopicProcessor,
    Processor.botscore : BotscoreProcessor,
    Processor.detect_search_terms: DetectSearchTerms,
    Processor.top_engagement: TopEngagementProcessor,
    Processor.detect_language: DetectLanguage
}
