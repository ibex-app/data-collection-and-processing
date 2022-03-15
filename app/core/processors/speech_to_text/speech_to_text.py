from cgitb import text
import requests
import base64
from sys import argv
from app.config.aop_config import slf, sleep_after
from ibex_models import ProcessTask, Post, Transcript
import subprocess
from app.config.constants import media_directory
import os
import json
import subprocess
from typing import List 


@slf
class SpeechToTextProcessor:
    
    async def process(self, task:ProcessTask):
        self.log.info(f'[Speech_to_text] {task.post.id} in process task')
        scenecuts = self.get_scenecuts(task.post)
        self.log.info(f'[Speech_to_text] {task.post.id} scenecuts')
        self.extract_audio(task.post)
        self.split_audio_to_scenes(task.post, scenecuts)
        
        transcripts = self.get_transcripts(task.post, scenecuts)
        self.log.info(f'[Speech_to_text] {task.post.id} transcripts')

        await self.save_post(task.post, transcripts)
        self.log.info(f'[Speech_to_text] {task.post.id} save_post')
        self.delete_audio_chunks(task.post)

        return True


    def convert_to_base64(self, audio_path):
        with open(audio_path, "rb") as audio_file:
            return base64.b64encode(audio_file.read()).decode("utf-8")
    
    
    def get_duration(self, post:Post):
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                "format=duration", "-of",
                                "default=noprint_wrappers=1:nokey=1", f'{media_directory}{post.id}.mp4'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        return float(result.stdout)


    def get_scenecuts(self, post:Post):
        process = subprocess.Popen(["python3", "-m", "scenecut_extractor", f'{media_directory}{post.id}.mp4'], 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        scenecuts = json.loads(stdout)
        
        # duration = self.get_duration(post)
        # scenecuts = []
        # sec = 0
        # while sec < duration:
        #     scenecuts.append({'pts_time': sec})
        #     sec += 160

        return scenecuts


    def extract_audio(self, post:Post):
        os.popen(f'mkdir -p {media_directory}{post.id}').read()
        command = f'ffmpeg -i {media_directory}{post.id}.mp4 -ab 160k -acodec pcm_s16le -ac 1 -ar 16000 -vn {media_directory}{post.id}/full.wav'
        os.popen(command).read()
        self.log.info(f'[Speech_to_text] audio extracted' )


    def split_audio_to_scenes(self, post:Post, scenecuts):
        for i, frame in enumerate(scenecuts):
            duration = 99999999 if i == len(scenecuts) - 1 else scenecuts[i + 1]["pts_time"] - frame["pts_time"]
            command = f'ffmpeg -ss {frame["pts_time"]} -i {media_directory}{post.id}/full.wav -t {duration} -c copy {media_directory}{post.id}/{frame["pts_time"]}s.wav'
            os.popen(command).read()

        self.log.info(f'[Speech_to_text] {len(scenecuts)} chuncks generated' )


    async def save_post(self, post:Post, transkripts:List[Transcript]):
        self.log.info(transkripts)
        post.transcripts = transkripts
        await post.save()


    def get_transcripts(self, post:Post, scenecuts):
        transcripts: List[Transcript] = []

        for frame in scenecuts:
            audio = self.convert_to_base64(f'{media_directory}{post.id}/{frame["pts_time"]}s.wav')
            self.log.info(f'{media_directory}{post.id}/{frame["pts_time"]}s.wav')
            transcript_text = requests.post('http://134.122.80.241:50001/', json={"audio": audio}).json()
            self.log.info(transcript_text)
            
            if transcript_text['text'].strip() != '':
                transcripts.append(Transcript(text=transcript_text['text'], second=frame["pts_time"]))

        return transcripts

    def delete_audio_chunks(self, post:Post):
        os.popen(f'ls -1 {media_directory}{post.id}| xargs rm -rfv').read()
        os.popen(f'rm -rf {media_directory}{post.id}').read()

