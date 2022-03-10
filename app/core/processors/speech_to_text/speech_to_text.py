import requests
import base64
from sys import argv
 

class SpeechToTextProcessor:
    def convert_to_base64(self, audio_path):
        with open(audio_path, "rb") as audio_file:
            return base64.b64encode(audio_file.read()).decode("utf-8")


    def process(self):
        audio = self.convert_to_base64(argv[1])

        req = requests.post('http://134.122.80.241:50001/', json={"audio": audio})
        print(req.text)
